package com.tuplejump.snackfs.cassandra.store

import java.io.InputStream
import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.UUID

import com.datastax.driver.core._
import com.datastax.driver.core.utils.Bytes
import com.tuplejump.snackfs.cassandra.model.{GenericOpSuccess, Keyspace, SnackFSConfiguration}
import com.tuplejump.snackfs.cassandra.partial.FileSystemStore
import com.tuplejump.snackfs.fs.model.{BlockMeta, INode, SubBlockMeta}
import com.tuplejump.snackfs.fs.stream.BlockInputStream
import com.tuplejump.snackfs.util.LogConfiguration
import com.twitter.logging.Logger
import org.apache.cassandra.utils.{ByteBufferUtil, FBUtilities, UUIDGen}
import org.apache.hadoop.fs.Path

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.util.{Failure, Success, Try}

class Cql3Store(configuration: SnackFSConfiguration) extends FileSystemStore {
  LogConfiguration.config()

  private lazy val log = Logger.get(getClass)

  private val GRACE_SECONDS: Int = 60

  private val SENTINEL_VALUE: String = "x"

  private val INODE_COLUMN_FAMILY_NAME = "inode"
  private val BLOCK_COLUMN_FAMILY_NAME = "sblock"
  private val LOCK_COLUMN_FAMILY_NAME = "createlock"

  private val cluster: Cluster = Cluster.builder
    .withClusterName("myCluster")
    .addContactPoint(configuration.CassandraHost)
    .withPort(configuration.CassandraCqlPort)
    .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM))
    .build

  private val session: Session = cluster.connect

  override def init: Unit = {}

  override def createKeyspace: Future[Keyspace] = {
    val result = promise[Keyspace]()
    val MIN_COMPACTION = 16
    val MAX_COMPACTION = 64

    Try {
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS ${configuration.keySpace} WITH REPLICATION = { 'class' : '${configuration.replicationStrategy}', 'replication_factor' : ${configuration.replicationFactor.toString} };")
      session.execute(s"USE ${configuration.keySpace} ;")

      session.execute(s"CREATE TABLE IF NOT EXISTS $INODE_COLUMN_FAMILY_NAME (id text, data blob, parentpath text, path text, sentinel text, PRIMARY KEY (id)) ;")
      session.execute(s"CREATE INDEX IF NOT EXISTS ON $INODE_COLUMN_FAMILY_NAME (parentpath) ;")
      session.execute(s"CREATE INDEX IF NOT EXISTS ON $INODE_COLUMN_FAMILY_NAME (path) ;")
      session.execute(s"CREATE INDEX IF NOT EXISTS ON $INODE_COLUMN_FAMILY_NAME (sentinel) ;")

      session.execute(s"CREATE TABLE IF NOT EXISTS $BLOCK_COLUMN_FAMILY_NAME (id blob, subid blob, data blob, PRIMARY KEY (id, subid)) WITH COMPACT STORAGE AND gc_grace_seconds = $GRACE_SECONDS AND compaction = { 'min_threshold': $MIN_COMPACTION, 'max_threshold': $MAX_COMPACTION, 'class': 'SizeTieredCompactionStrategy' }")
      session.execute(s"CREATE TABLE IF NOT EXISTS $LOCK_COLUMN_FAMILY_NAME (id blob, val blob, process blob, PRIMARY KEY (id, val)) WITH COMPACT STORAGE AND gc_grace_seconds = $GRACE_SECONDS AND compaction = { 'min_threshold': $MIN_COMPACTION, 'max_threshold': $MAX_COMPACTION, 'class': 'SizeTieredCompactionStrategy' }")
    } match {
      case Success(_) =>
        result success new Keyspace(configuration.keySpace)
      case Failure(e) =>
        result failure e
    }
    result.future
  }

  def dropKeyspace: Future[Unit] = {
    val result = promise[Unit]()
    Try {
      session.execute(s"DROP KEYSPACE ${configuration.keySpace} ;")
    } match {
      case Success(_) =>
        result success GenericOpSuccess()
      case Failure(e) =>
        result failure e
    }
    result.future
  }

  override def storeINode(path: Path, iNode: INode): Future[GenericOpSuccess] = {
    val result = promise[GenericOpSuccess]()
    val data: ByteBuffer = iNode.serialize
    val timestamp = iNode.timestamp

    //todo
    Try {
      session.execute(s"INSERT INTO $INODE_COLUMN_FAMILY_NAME (id, data, parentpath, path, sentinel) VALUES ('${getPathKey(path)}', ${Bytes.toHexString(data)}, '${getParentForIndex(path)}', '${path.toUri.getPath}', '$SENTINEL_VALUE') USING TIMESTAMP $timestamp")
    } match {
      case Success(_) =>
        result success GenericOpSuccess()
      case Failure(e) =>
        result failure e
    }
    result.future
  }

  override def retrieveINode(path: Path): Future[INode] = {
    val result = promise[INode]()
    val pathKey: String = getPathKey(path)
    Try {
      session.execute(s"SELECT data, writetime(data) as ts from $INODE_COLUMN_FAMILY_NAME where id='$pathKey';")
    } match {
      case Success(resultSet) =>
        if (resultSet.isExhausted) result failure new RuntimeException("No inode retrieved")
        val row = resultSet.one
        result success INode.deserialize(ByteBufferUtil.inputStream(row.getBytes("data")), row.getTime("ts"))

      case Failure(e) =>
        result failure e
    }
    result.future
  }

  override def storeSubBlock(blockId: UUID, subBlockMeta: SubBlockMeta, data: ByteBuffer): Future[GenericOpSuccess] = {
    val result = promise[GenericOpSuccess]()
    val parentBlockId: ByteBuffer = ByteBufferUtil.bytes(blockId)
    val subBlockId: ByteBuffer = ByteBufferUtil.bytes(subBlockMeta.id)

    //todo
    Try {
      session.execute(s"INSERT INTO $BLOCK_COLUMN_FAMILY_NAME (id, subid, data) VALUES (${Bytes.toHexString(parentBlockId)}, ${Bytes.toHexString(subBlockId)}, ${Bytes.toHexString(data)}) USING TIMESTAMP ${System.currentTimeMillis}")
    } match {
      case Success(_) =>
        result success GenericOpSuccess()
      case Failure(e) =>
        result failure e
    }
    result.future
  }

  override def retrieveSubBlock(blockId: UUID, subBlockId: UUID, byteRangeStart: Long): Future[InputStream] = {
    val result = promise[InputStream]()
    val blockIdBuffer: ByteBuffer = ByteBufferUtil.bytes(blockId)
    val subBlockIdBuffer = ByteBufferUtil.bytes(subBlockId)
    Try {
      session.execute(s"SELECT data from $BLOCK_COLUMN_FAMILY_NAME where id = ${Bytes.toHexString(blockIdBuffer)} AND subid = ${Bytes.toHexString(subBlockIdBuffer)}")
    } match {
      case Success(resultSet) =>
        if (resultSet.isExhausted) result failure new RuntimeException("No sblock retrieved")
        else result success ByteBufferUtil.inputStream(resultSet.one.getBytes("data"))

      case Failure(e) =>
        result failure e
    }
    result.future
  }

  override def retrieveBlock(blockMeta: BlockMeta): InputStream = {
    log.debug("retrieve Block %s", blockMeta.toString)
    BlockInputStream(this, blockMeta, configuration.atMost)
  }

  override def deleteINode(path: Path): Future[GenericOpSuccess] = {
    val result = promise[GenericOpSuccess]()
    Try {
      session.execute(s"DELETE FROM $INODE_COLUMN_FAMILY_NAME USING TIMESTAMP ${System.currentTimeMillis} WHERE id = '${getPathKey(path)}'")
    } match {
      case Success(_) =>
        result success GenericOpSuccess()
      case Failure(e) =>
        result failure e
    }
    result.future
  }

  override def deleteBlocks(iNode: INode): Future[GenericOpSuccess] = {
    val result = promise[GenericOpSuccess]()
    val blockIds = iNode.blocks.map(block => Bytes.toHexString(ByteBufferUtil.bytes(block.id)))
    Try {
      session.execute(s"DELETE FROM $BLOCK_COLUMN_FAMILY_NAME USING TIMESTAMP ${System.currentTimeMillis} WHERE id IN (${blockIds.mkString(", ")})")
    } match {
      case Success(_) =>
        result success GenericOpSuccess()
      case Failure(e) =>
        result failure e
    }
    result.future
  }

  override def fetchSubPaths(path: Path, isDeepFetch: Boolean): Future[Set[Path]] = {
    val result = promise[Set[Path]]()
    val startPath = path.toUri.getPath
    val searchExpression: String = {
      if (isDeepFetch) {
        s"path > '$startPath'" + {
          if (startPath.length > 1) s"AND path < '${getEndPath(startPath)}'" else ""
        }
      } else {
        s"parentpath = '$startPath'"
      }

    } + s" AND sentinel='$SENTINEL_VALUE' ALLOW FILTERING"

    def recursionStrategy: String = {
      if (isDeepFetch) {
        "recursively"
      } else {
        "non-recursively"
      }
    }
    log.debug("fetching subPaths for %s, %s ", path, recursionStrategy)

    Try {
      session.execute(s"SELECT path FROM $INODE_COLUMN_FAMILY_NAME WHERE $searchExpression")
    } match {
      case Success(rs) =>
        result success (for {
          row <- rs.asScala
        } yield new Path(row.getString("path"))).toSet
      case Failure(e) =>
        result failure e
    }
    result.future
  }

  override def getBlockLocations(path: Path): Future[Map[BlockMeta, List[String]]] = {
    val result = promise[Map[BlockMeta, List[String]]]()
    val inodeFuture = retrieveINode(path)
    inodeFuture.onFailure {
      case e =>
        result failure e
    }

    inodeFuture.onSuccess {
      case inode =>
        log.debug("found iNode for %s, getting block locations", path)

        val metadata = cluster.getMetadata

        result success (for {
          b <- inode.blocks
        } yield (b -> metadata.getReplicas(configuration.keySpace, ByteBufferUtil.bytes(b.id)).asScala.toList.map(_.getAddress.getHostAddress))).toMap
    }
    result.future
  }

  override def acquireFileLock(path: Path, processId: UUID): Future[Boolean] = {
    val prom = promise[Boolean]()
    log.debug("adding column for create lock")
    val key = getPathKey(path)
    val timeStamp = UUIDGen.getTimeUUID
    Try {
      session.execute(s"INSERT INTO $LOCK_COLUMN_FAMILY_NAME (id, val, process) VALUES (${Bytes.toHexString(key.getBytes)}, ${Bytes.toHexString(ByteBufferUtil.bytes(timeStamp))}, ${Bytes.toHexString(ByteBufferUtil.bytes(processId))}) USING TIMESTAMP ${System.currentTimeMillis}")
    } match {
      case Success(_) =>
        log.debug("added column for create lock")
        log.debug("getting row for create lock")
        Try {
          session.execute(s"SELECT process from $LOCK_COLUMN_FAMILY_NAME where id = ${Bytes.toHexString(key.getBytes)}")
        } match {
          case Success(resultSet) =>
            val rows = resultSet.all()
            if (rows.size() >= 1) {
              val processIds = rows.asScala.map(r => new String(r.getBytes("process").array()))
              val currentId = new String(ByteBufferUtil.bytes(processId).array())
              prom success processIds.forall(_ == currentId)
            } else {
              prom success false
            }
          case Failure(e) =>
            prom failure e
        }
      case Failure(e) =>
        prom failure e
    }
    prom.future
  }

  override def releaseFileLock(path: Path): Future[Boolean] = {
    val prom = promise[Boolean]()
    Try {
      session.execute(s"DELETE FROM $LOCK_COLUMN_FAMILY_NAME USING TIMESTAMP ${System.currentTimeMillis} WHERE id = ${Bytes.toHexString(getPathKey(path).getBytes)}")
    } match {
      case Success(_) =>
        prom success true
      case Failure(e) =>
        prom success false
    }
    prom.future
  }


  private def getEndPath(startPath: String): String = {
    val lastChar = (startPath(startPath.length - 1) + 1).asInstanceOf[Char]
    startPath.substring(0, startPath.length - 1) + lastChar
  }

  private def getPathKey(path: Path): String = {
    val pathBytes: ByteBuffer = ByteBufferUtil.bytes(path.toUri.getPath)
    val pathBytesAsInt: BigInteger = FBUtilities.hashToBigInteger(pathBytes)
    pathBytesAsInt.toString(16)
  }

  private def getParentForIndex(path: Path): String = {
    val parent = path.getParent
    var result = "null"
    if (parent != null) {
      result = parent.toUri.getPath
    }
    result
  }
}
