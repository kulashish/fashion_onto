package com.jabong.dap.model.product.itr

import com.jabong.dap.common.Spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scala.runtime.RichLong

object Bob {

  /**
   * Prepare data frame of bob related columns
   * Note: All column names are following camel casing
   * pattern
   *
   * @return DataFrame
   */
  def getBobColumns(): DataFrame = {
    val stockDataFrame = stockDF()
    val visibilityDataFrame = visibilityDF()

    val bob = Model.simple.select(
      "id_catalog_simple",
      "special_price",
      "special_to_date",
      "special_from_date",
      "sku",
      "fk_catalog_config"
    ).withColumnRenamed("sku", "simpleSku").
      join(Model.config.select(
        "id_catalog_config",
        "name",
        "special_margin",
        "margin",
        "activated_at",
        "sku",
        "fk_catalog_supplier",
        "fk_catalog_brand"
      ), Model.config("id_catalog_config") === Model.simple("fk_catalog_config")).
      join(
        Model.supplier.value.select("status", "id_catalog_supplier").withColumnRenamed("status", "supplierStatus"),
        Model.config("fk_catalog_supplier") === Model.supplier.value("id_catalog_supplier")
      ).
        join(
          Model.brand.value.select("url_key", "id_catalog_brand").withColumnRenamed("url_key", "brandUrlKey"),
          Model.config("fk_catalog_brand") === Model.brand.value("id_catalog_brand")
        ).join(
            visibilityDataFrame,
            visibilityDataFrame.col("idCatalogSimple") === Model.simple.col("id_catalog_simple"),
            "left_outer"
          ).join(
              stockDataFrame,
              stockDataFrame.col("idCatalogSimple") === Model.simple.col("id_catalog_simple"),
              "left_outer"
            ).
              select(
                "id_catalog_config",
                "name",
                "special_margin",
                "margin",
                "activated_at",
                "sku",
                "id_catalog_simple",
                "special_price",
                "special_to_date",
                "special_from_date",
                "simpleSku",
                "supplierStatus",
                "brandUrlKey",
                "visibility",
                "quantity"
              ).
                withColumnRenamed("id_catalog_config", "idCatalogConfig").
                withColumnRenamed("name", "productName").
                withColumnRenamed("special_margin", "specialMargin").
                withColumnRenamed("activated_at", "activatedAt").
                withColumnRenamed("id_catalog_simple", "idCatalogSimple").
                withColumnRenamed("special_price", "specialPrice").
                withColumnRenamed("special_to_date", "specialToDate").
                withColumnRenamed("special_from_date", "specialFromDate")

    val productUrl = udf(url)

    bob.
      withColumn(
        "productUrl",
        productUrl(col("idCatalogConfig"), col("brandUrlKey"), col("productName"))
      )
  }

  /**
   * Data frame of stock columns of data frame sku & quantity
   *
   * @return DataFrame
   */
  def stockDF(): DataFrame = {
    val reservedDF = simpleReservedDF()
    val stockUDF = udf(stock)

    Model.simple.select(
      "id_catalog_simple",
      "sku"
    ).join(
        Model.catalogStock.select("fk_catalog_simple", "quantity"),
        Model.simple.col("id_catalog_simple") === Model.catalogStock.col("fk_catalog_simple"),
        "left_outer"
      ).join(
          reservedDF,
          Model.simple.col("sku") === reservedDF.col("simpleSku")
        ).withColumn(
            "quantity",
            stockUDF(col("quantity"), col("is_reserved"))
          ).select(
              "id_catalog_simple",
              "quantity"
            ).withColumnRenamed(
                "id_catalog_simple",
                "idCatalogSimple"
              )
  }

  /**
   * Calculate stock against simple sku
   *
   * @return Int
   */
  val stock = (quantity: Int, reservedCount: Int) => {
    quantity - reservedCount
  }

  /**
   * Prepare front end product url
   *
   * @return String
   */
  val url = (idCatalogConfig: Long, brandUrlKey: String, productName: String) => {
    ("%s-%s-d").format(brandUrlKey.replaceAll("/", ""), productName.replaceAll(" ", "-"), idCatalogConfig)
  }

  def simpleReservedDF(): DataFrame = {
    Model.salesOrderItem.filter("is_reserved = 1").groupBy("sku").agg(count("is_reserved") as "is_reserved").withColumnRenamed("sku", "simpleSku")
  }

  /**
   * Bob visibility data frame of simples
   * columns of data frame are "idCatalogSimple" & "visibility"
   *
   * @return DataFrame
   */
  def visibilityDF(): DataFrame = {
    val categoryMapping = Model.category.value.select("status", "id_catalog_category").
      withColumnRenamed("status", "categoryStatus").
      join(
        Model.categoryMapping.select("fk_catalog_category", "fk_catalog_config"),
        Model.categoryMapping.col("fk_catalog_category") === Model.category.value.col("id_catalog_category")
      ).select("categoryStatus", "fk_catalog_config")

    val image = Model.productImage.groupBy("fk_catalog_config").agg(count("image") as "image_count")

    val reservedDF = simpleReservedDF()
    val ss = Model.simple.select(
      "sku",
      "fk_catalog_config",
      "id_catalog_simple"
    ).withColumnRenamed("sku", "simpleSku").
      join(Model.config.select(
        "id_catalog_config",
        "status",
        "status_supplier_config",
        "fk_catalog_supplier",
        "pet_approved",
        "fk_catalog_brand"
      ), Model.simple.col("fk_catalog_config") === Model.config.col("id_catalog_config")).
      join(
        Model.supplier.value.select("status", "id_catalog_supplier").
          withColumnRenamed("status", "supplierStatus"),
        Model.config.col("fk_catalog_supplier") === Model.supplier.value.col("id_catalog_supplier")
      ).join(
          image,
          image.col("fk_catalog_config") === Model.config.col("id_catalog_config")
        ).join(
            Model.brand.value.select("status", "id_catalog_brand")
              .withColumnRenamed("status", "brandStatus"),
            Model.brand.value.col("id_catalog_brand") === Model.config.col("fk_catalog_brand"),
            "left_outer"
          )

    val visibilityUDF = udf(simpleVisibility)
    ss.join(
      reservedDF,
      ss.col("simpleSku") === reservedDF.col("simpleSku"), "left_outer"
    ).join(
        categoryMapping.select("categoryStatus", "fk_catalog_config"),
        categoryMapping.col("fk_catalog_config") === ss.col("id_catalog_config"),
        "left_outer"
      ).join(
          Model.catalogStock.select("fk_catalog_simple", "quantity"),
          Model.catalogStock.col("fk_catalog_simple") === ss.col("id_catalog_simple"),
          "left_outer"
        ).select(
            "id_catalog_simple",
            "status",
            "categoryStatus",
            "supplierStatus",
            "brandStatus",
            "status_supplier_config",
            "pet_approved",
            "image_count",
            "is_reserved",
            "quantity"
          ).withColumn(
              "visibility",
              visibilityUDF(
                col("status"),
                col("categoryStatus"),
                col("supplierStatus"),
                col("brandStatus"),
                col("status_supplier_config"),
                col("pet_approved"),
                col("image_count"),
                col("is_reserved"),
                col("quantity")
              )
            ).select(
                "id_catalog_simple",
                "visibility"
              ).
                withColumnRenamed(
                  "id_catalog_simple",
                  "idCatalogSimple"
                )

  }

  /**
   * Calculate visibility based upon the input parameter passed.
   *
   * status : status column of value of catalog_config of bob
   * categoryStatus : status column of catalog_category of bob
   * supplierStatus : status column of catalog_supplier of bob
   * brandStatus : status column of catalog_brand of bob
   * statusSupplierConfig : status_supplier_config of catalog_config of bob
   * petApproved : pet_approved column of catalog_config of bob
   * imageCount : total number of images again a product (count from catalog_product_image)
   * isReservedCount : total number of reserved count of sales_order_item
   * quantity : quantity column of catalog_stock of bob
   *
   */
  val simpleVisibility = (status: String, categoryStatus: String, supplierStatus: String, brandStatus: String, statusSupplierConfig: String, petApproved: Any, imageCount: Any, isReservedCount: Any, quantity: Any) => {
    var stock: Long = 0
    if (quantity != null && isReservedCount != null) {
      stock = quantity.toString.toLong - isReservedCount.toString.toLong
    }
    if (status == "active" &&
      categoryStatus == "active" &&
      supplierStatus == "active" &&
      brandStatus == "status" &&
      statusSupplierConfig == "active" &&
      petApproved.toString.toLong == 1 &&
      imageCount.toString.toLong > 0 &&
      stock > 0) {
      true
    } else {
      false
    }
  }
}
