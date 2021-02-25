import org.apache.spark.sql.Row
import java.io._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructField}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import java.text.SimpleDateFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.sql.Timestamp

import java.sql.Timestamp
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.orc._


import org.apache.kudu.spark.kudu._
import scala.sys.process._

//import spark.implicits._
//val sqlContext = new org.apache.spark.sql.SQLContext(sc)

object Write_kudu{
	def main(args: Array[String]) {

		
		//iniciar variables
		val (log_level, spark_master, path_table, path_nt, path_batch, kudu_master, ss_batch_secs, spark_log_level, group_id, security, sasl_mechanism, identificador) = StreamingInit.configInit()
		val cmd = "kinit pycjulio -kt pycjulio.keytab"
		val output = cmd.!!
		Utils.printlog(output)

		if (log_level=="DEBUG") {Utils.printlog(" DEBUG StreamingX: Variables inicializadas")}
		// if (log_level=="INFO" || log_level=="DEBUG") {Utils.printlog(" INFO StreamingX: Inicio del proceso, topico(s) a consumir: "+ args(0))}

		//iniciar sesion y contexto de spark
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG: Iniciando session spark")}   
		val spark = SparkSession.builder.master(spark_master).getOrCreate()
		spark.sparkContext.setLogLevel(spark_log_level)
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG: Spark iniciado correctamente")} 
		  
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG: Iniciando contexto de spark")}   
		import spark.implicits._
		val sc = spark.sparkContext
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG: Session y contexto de spark creado")}

		// Inicializando SparkStreaming 
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG StreamingX: Inicializando SparkStreaming")}
		// val streamingContext = new StreamingContext(spark.sparkContext, Milliseconds(ss_batch_secs.toLong))
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG StreamingX: SparkStreaming iniciado correctamente")}

		import spark.implicits._

		
		val new_df = spark.read.format("org.apache.kudu.spark.kudu").options(Map("kudu.master" -> kudu_master, "kudu.table" -> path_table)).load
		// val new_df = spark.read.format("csv").option("header","true").option("delimiter",";").load(path_table)
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG StreamingX: ===> Csv cargado")}

		// var n_df = wl_table.select("periodo","indice_xml","fecha_cargue_kudu","nro_tx","cod_respuesta","mensaje_respuesta","nro_identificacion","tipo_identificacion","descripcion_campana","tipo_campana","canal_redencion","cupo_preaprobado_producto","rpr_base","rpr_fecha","producto_movil","familia_producto","caracteristica_1","caracteristica_2","caracteristica_3","fecha_calificacion_base","fecha_inicio_campana","hora_inicio_campana","fecha_fin_campana","hora_fin_campana","flg_campana_vigente","cod_promocion","cod_aliado","score_1_aliado","score_2_aliado","score_3_aliado","score_4_aliado","score_5_aliado","score_6_aliado","score_7_aliado","flg_calificado_aliado","tasa_whitelist","grupo_riesgo")
		
		new_df.write.format("org.apache.kudu.spark.kudu").options(Map("kudu.master" -> kudu_master, "kudu.table" -> path_nt)).mode("append").save()
		if (log_level=="INFO" || log_level=="DEBUG" || log_level=="OUTPUT" ){print(" INFO StreamingX: Successfully written: "+new_df.count()+" records")}
	}
}