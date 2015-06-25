package com.jabong.dap.campaign.recommendation

import scala.collection.immutable.HashMap

/**
 * Created by jabong1145 on 22/6/15.
 */
trait Recommender  extends java.io.Serializable{

  var RecommendationGenderMap = new HashMap[String,String]
  RecommendationGenderMap+=(
    "WOMEN"       -> "WOMEN,UNISEX",
    "MEN"         -> "MEN,UNISEX",
    "GIRLS"       -> "GIRLS,BOYS GIRLS",
    "UNISEX"      -> "UNISEX,MEN,WOMEN",
    "BOYS GIRLS"  -> "WOMEN,UNISEX",
    "BOYS"        -> "BOYS,BOYS GIRLS",
    "Infant"      -> "Infant",
    "Blank"       -> "Blank"
    )

  def getRecommendationGender(gender:Any): String ={
    if(gender==null){
      return null
    }
    return  RecommendationGenderMap.getOrElse(gender.toString,null)
  }


}
