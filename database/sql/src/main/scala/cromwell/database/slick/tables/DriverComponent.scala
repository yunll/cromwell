package cromwell.database.slick.tables

import java.sql.{Blob, Clob}
import javax.sql.rowset.serial.{SerialBlob, SerialClob}
import slick.jdbc.JdbcProfile

trait DriverComponent {
  val driver: JdbcProfile

  import driver.api._

  implicit val serialClobColumnType = MappedColumnType.base[SerialClob, Clob](
    identity,
    {
      case serialClob: SerialClob => serialClob
      case clob => Option(clob).map(new SerialClob(_)).orNull
    }
  )

  implicit val serialBlobColumnType = MappedColumnType.base[SerialBlob, Blob](
    identity,
    {
      case serialBlob: SerialBlob => serialBlob
      case blob => Option(blob).map(new SerialBlob(_)).orNull
    }
  )
}
