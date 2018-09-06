package org.thp.scalligraph

package object query {
  implicit class SearchField(field: String) {

    def ~=(value: Any): PredicateFilter[Nothing] = Filter.is(field, value)
//    def like(value: String) = Like(field, value)
    def ~!=(value: Any): PredicateFilter[Nothing] = Filter.neq(field, value)
    def ~<(value: Any): PredicateFilter[Nothing]  = Filter.lt(field, value)
    def ~>(value: Any): PredicateFilter[Nothing]  = Filter.gt(field, value)
    def ~<=(value: Any): PredicateFilter[Nothing] = Filter.lte(field, value)
    def ~>=(value: Any): PredicateFilter[Nothing] = Filter.gte(field, value)
    //    def ~<>(value: (Any, Any)) =
    //    def ~=<>=(value: (Any, Any)) =
    //    def in(values: AnyRef*) =
  }

}
