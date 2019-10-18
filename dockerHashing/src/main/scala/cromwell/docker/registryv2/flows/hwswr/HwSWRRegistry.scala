package cromwell.docker.registryv2.flows.hwswr

import java.math.BigInteger
import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import cats.effect.IO
import com.google.gson
import cromwell.docker.DockerInfoActor._
import cromwell.docker._
import cromwell.docker.registryv2.DockerRegistryV2Abstract
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import org.apache.http.HttpResponse
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.http4s.{Header, Request, Response, Uri}
import org.http4s.Uri.{Authority, Scheme}
import org.http4s.client.Client

import scala.util.matching.Regex


class HwSWRRegistry(config: DockerRegistryConfig) extends DockerRegistryV2Abstract(config) {
  val ProductName = "swr"
  val HashAlg = "sha256"
  val regionPattern = """[^\s]+"""
  val validSWRHosts: Regex = s"""swr.($regionPattern).myhuaweicloud.com""".r

  def isValidSWRHost(host: Option[String]): Boolean = {
    host.exists {
      _ match {
        case validSWRHosts(_) => true
        case _ => false
      }
    }
  }

  // Execute a request. No retries because they're expected to already be handled by the client
  def executeRequest[A](request: IO[Request[IO]], handler: Response[IO] => IO[A])(implicit client: Client[IO]): IO[A] = {
    client.fetch[A](request)(handler)
  }

  override def accepts(dockerImageIdentifier: DockerImageIdentifier): Boolean = isValidSWRHost(dockerImageIdentifier.host)

  override protected def getToken(dockerInfoContext: DockerInfoContext)(implicit client: Client[IO]): IO[Option[String]] = {
    // get UTC time
    val timeZone = TimeZone.getTimeZone("GMT");
    val simpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val shortSimpleDateFormat = new SimpleDateFormat("HHmmss")
    simpleDateFormat.setTimeZone(timeZone)
    shortSimpleDateFormat.setTimeZone(timeZone)
    val date = new Date()
    val time = simpleDateFormat.format(date)
    val shortTime = shortSimpleDateFormat.format(date)
    val signTime = time + "T" + shortTime + "Z"

    val hwsDate = "X-Hws-Date"
    val region = ""
    val service = ""
    val terminalString = "hws_request"
    val authHeaderPrefix = "HWS-HMAC-SHA256"
    val hwsName = "HWS"
    val query = ""

    val accessKey = dockerInfoContext.credentials(0).toString
    val securityKey = dockerInfoContext.credentials(1).toString
    val projectName = dockerInfoContext.credentials(2).toString
    val iamURL = s"https://iam.${projectName}.myhuaweicloud.com"

    val signedHeaders = "x-hws-date"
    val canonicalHeadersOut = "x-hws-date:" + signTime + "\n"

    val requestBody = """{"auth":{"identity":{"methods":["hw_access_key"],"hw_access_key":{"access":{"key":"""" + accessKey + """"}}},"scope":{"project":{"name":"""" + projectName + """"}}}}"""
    val hexBody = String.format("%032x", new BigInteger(1, MessageDigest.getInstance("SHA-256").digest(requestBody.toString.getBytes("UTF-8"))))

    val canonicalRequestStr = "POST" + "\n" + "/v3/auth/tokens/" + "\n" + query + "\n" + canonicalHeadersOut + "\n" + signedHeaders + "\n" + hexBody
    val canonicalRequestStrToStr = String.format("%032x", new BigInteger(1, MessageDigest.getInstance("SHA-256").digest(canonicalRequestStr.toString.getBytes("UTF-8"))))

    val credentialString = time + "/" + region + "/" + service + "/" + terminalString
    val stringToSign =
      s"""${authHeaderPrefix}
         |${signTime}
         |${credentialString}
         |${canonicalRequestStrToStr}""".stripMargin

    // hmac256
    val secret = hwsName + securityKey
    val timeData = getHmacSH256(secret.getBytes("UTF-8"), time.getBytes("UTF-8"))
    val regionData = getHmacSH256(timeData, region.getBytes("UTF-8"))
    val serviceData = getHmacSH256(regionData, service.getBytes("UTF-8"))
    val credentials = getHmacSH256(serviceData, terminalString.getBytes("UTF-8"))
    val toSignature = getHmacSH256(credentials, stringToSign.getBytes("UTF-8"))
    val signature = toSignature.map("%02x" format _).mkString
    val signHeader = authHeaderPrefix + " Credential=" + accessKey + "/" + credentialString + ", " + "SignedHeaders=" +  signedHeaders + ", " + "Signature=" + signature

    // http post
    val httpclient = HttpClients.createDefault()
    val post = new HttpPost(iamURL + "/v3/auth/tokens")
    post.setHeader("X-Identity-Sign", signHeader)
    post.setHeader(hwsDate, signTime)
    post.setHeader("Content-Type", "application/json;charset=utf8")
    post.setEntity(new StringEntity(requestBody, "UTF-8"))
    val response = httpclient.execute(post)
    for (header <- response.getAllHeaders) {
      if (header.getName() == "X-Subject-Token") {
        return IO(header.getValue()).map(Option.apply)
      }
    }
    IO(None)
//    IO.raiseError(new Exception("using ak/sk to get token from IAM Failed."))
  }

  def getHmacSH256(key: Array[Byte], content: Array[Byte]): Array[Byte] = {
    val secret = new SecretKeySpec(key, "HmacSHA256")
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(secret)
    mac.doFinal(content)
  }

  override protected def registryHostName(dockerImageIdentifier: DockerImageIdentifier): String = ""
  override protected def authorizationServerHostName(dockerImageIdentifier: DockerImageIdentifier): String = ""
  override protected def buildTokenRequestHeaders(dockerInfoContext: DockerInfoContext): List[Header] = List.empty

  override protected def getDockerResponse(token: Option[String], dockerInfoContext: DockerInfoContext)(implicit client: Client[IO]): IO[DockerInfoSuccessResponse] = {
    val region = dockerInfoContext.credentials(2).toString

    val httpclient = HttpClients.createDefault()
    val post = new HttpGet(buildDigestUri(dockerInfoContext, region).renderString)
    post.setHeader("X-Auth-Token", token.get)
    post.setHeader("Content-Type", "application/json;charset=utf8")
    post.setHeader("ProjectName", region)
    post.setHeader("Region", region)
    for {
      response <- IO.pure(httpclient.execute(post))
      dockerResponse <- handleResponse(response, dockerInfoContext)
    } yield dockerResponse
  }

  private def handleResponse(response: HttpResponse, dockerInfoContext: DockerInfoContext): IO[DockerInfoSuccessResponse] = {
    if(isSuccess(response.getStatusLine.getStatusCode)) {
      val ret = EntityUtils.toString(response.getEntity, "UTF-8")
      val list = new gson.JsonParser().parse(ret).getAsJsonArray
      list.forEach(el => {
        val digest = el.getAsJsonObject.get("digest").getAsString.substring(HashAlg.length+1)
        val size = el.getAsJsonObject.get("size").getAsLong
        return IO(DockerInfoSuccessResponse(DockerInformation(DockerHashResult(HashAlg, digest), Option(DockerSize(size))), dockerInfoContext.request))
      })
      IO.raiseError(new Exception("Not Found"))
    }
    IO.raiseError(new Exception(s"Qurey digest failed: ${response.getStatusLine.getReasonPhrase}"))
  }

  private def buildDigestUri(context: DockerInfoContext, region: String): Uri = {
    val projectName = context.dockerImageID.repository.get
    val name = context.dockerImageID.image
    val tag = context.dockerImageID.reference
    Uri.apply(
      scheme = Option(Scheme.https),
      authority = Option(Authority(host = Uri.RegName(s"swr-api.$region.myhuaweicloud.com"))),
      path = s"/v2/manage/namespaces/${projectName}/repos/${name}/tags?filter=tag::${tag}"
    )
  }

  private def isSuccess(code: Int): Boolean = {
    200 <= code && code < 300
  }

}
