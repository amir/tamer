package tamer.oci

import zio.Ref

package object objectstorage {
  type NamesR = Ref[List[String]]
  type Names  = List[String]
}
