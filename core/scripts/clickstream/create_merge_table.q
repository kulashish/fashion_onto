CREATE DATABASE IF NOT EXISTS merge;
USE merge;

CREATE EXTERNAL TABLE merge_pagevisit(
id                  string,
browserid           string,
visitid             string,
userid              string,
actualvisitid       string,
channel             string,
derivedcategory     string,
pagets              timestamp,
ip                  string,
categories          array<string>,
url                 string,
pagetype            string,
referrer            string,
brand               string,
sessionid           string,
ajax                boolean,
formula             string,
productsku          string,
impressions         array<struct<sku:string,srow:string,scol:string>>,
vouchercode         string,
voucherfailedreason string,
cartskus            array<string>,
corder              struct<id:string,totalprice:float,vouchercode:string,paymentmode:string,firstorder:boolean,products:array<struct<sku:string,price:float>>>,
position            struct<srow:string,scol:string>,
domain              string,
device              string,
suggestions         struct<typedterm:string,clickedterm:string,rank:string,suggestionslist:array<string>>,
useragent           string,
visitts             timestamp,
usermatch           string,
appstoreemail       string,
internetconnection  string,
mobileos            string,
installdate         string,
appversion          string,
loginmode           string,
add4push            string
)
partitioned by (date1 string, month1 string, year1 string) STORED AS ORC LOCATION '/data/clickstream/merge'