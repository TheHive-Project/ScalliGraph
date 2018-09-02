package org.thp.scalligraph

package object query {
  implicit class SearchField(field: String) {

    def ~=(value: Any) = Filter.is(field, value)
//    def like(value: String) = Like(field, value)
    def ~!=(value: Any) = Filter.neq(field, value)
    def ~<(value: Any)  = Filter.lt(field, value)
    def ~>(value: Any)  = Filter.gt(field, value)
    def ~<=(value: Any) = Filter.lte(field, value)
    def ~>=(value: Any) = Filter.gte(field, value)
    //    def ~<>(value: (Any, Any)) =
    //    def ~=<>=(value: (Any, Any)) =
    //    def in(values: AnyRef*) =
  }

}
