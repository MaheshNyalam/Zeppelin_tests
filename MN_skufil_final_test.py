﻿sku_master_txt=sqlContext.read.format("csv").option("header", "false").option("inferSchema", "true").option("delimiter", '|').load("s3://aap-data-exploration/mahesh.nyalam/SKUFIL-08-26-21.TXT")
sku_master_txt.registerTempTable("sku_master_txt")
sku_master_delta=spark.sql("select ' ' as sku_rk, CASE WHEN _c0 =' ' THEN NULL ELSE _c0 END as skunum, CASE WHEN _c1 =' ' THEN null ELSE trim(_c1 ) END as sfnump, CASE WHEN _c2 =' ' THEN null ELSE trim(_c2 ) END as sfnumw, CASE WHEN _c3 =' ' THEN null ELSE trim(_c3 ) END as sfdesc, CASE WHEN _c4 =' ' THEN null ELSE trim(_c4 ) END as sfdept, CASE WHEN _c5 =' ' THEN null ELSE trim(_c5 ) END as sfclas, CASE WHEN _c6 =' ' THEN null ELSE trim(_c6 ) END as sfscls, CASE WHEN _c7 =' ' THEN null ELSE trim(_c7 ) END as sfmfg1, CASE WHEN _c8 =' ' THEN null ELSE trim(_c8 ) END as sfmfg2, CASE WHEN _c9 =' ' THEN null ELSE trim(_c9 ) END as sfmfg3, CASE WHEN _c10=' ' THEN null ELSE trim(_c10) END as sfvndr, CASE WHEN _c11=' ' THEN null ELSE trim(_c11) END as sfdisc, CASE WHEN _c12=' ' THEN null ELSE trim(_c12) END as sfhazc, CASE WHEN _c13=' ' THEN null ELSE trim(_c13) END as sfcost, CASE WHEN _c14=' ' THEN null ELSE trim(_c14) END as sfccs1, CASE WHEN _c15=' ' THEN null ELSE trim(_c15) END as sfccs2, CASE WHEN _c16=' ' THEN null ELSE trim(_c16) END as sfccs3, CASE WHEN _c17=' ' THEN null ELSE trim(_c17) END as sfccs4, CASE WHEN _c18=' ' THEN null ELSE trim(_c18) END as sfccs5, CASE WHEN _c19=' ' THEN null ELSE trim(_c19) END as sfccdt, CASE WHEN _c20=' ' THEN null ELSE trim(_c20) END as sfccd1, CASE WHEN _c21=' ' THEN null ELSE trim(_c21) END as sfccd2, CASE WHEN _c22=' ' THEN null ELSE trim(_c22) END as sfccd3, CASE WHEN _c23=' ' THEN null ELSE trim(_c23) END as sfccd4, CASE WHEN _c24=' ' THEN null ELSE trim(_c24) END as sfccd5, CASE WHEN _c25=' ' THEN null ELSE trim(_c25) END as sfcorc, CASE WHEN _c26=' ' THEN null ELSE trim(_c26) END as sfcccd, CASE WHEN _c27=' ' THEN null ELSE trim(_c27) END as sfccc1, CASE WHEN _c28=' ' THEN null ELSE trim(_c28) END as sfccc2, CASE WHEN _c29=' ' THEN null ELSE trim(_c29) END as sfccc3, CASE WHEN _c30=' ' THEN null ELSE trim(_c30) END as sfccc4, CASE WHEN _c31=' ' THEN null ELSE trim(_c31) END as sfccc5, CASE WHEN _c32=' ' THEN null ELSE trim(_c32) END as sfret, CASE WHEN _c33=' ' THEN null ELSE trim(_c33) END as sfret1, CASE WHEN _c34=' ' THEN null ELSE trim(_c34) END as sfret2, CASE WHEN _c35=' ' THEN null ELSE trim(_c35) END as sfret3, CASE WHEN _c36=' ' THEN null ELSE trim(_c36) END as sfret4, CASE WHEN _c37=' ' THEN null ELSE trim(_c37) END as sfret5, CASE WHEN _c38=' ' THEN null ELSE trim(_c38) END as sfretd, CASE WHEN _c39=' ' THEN null ELSE trim(_c39) END as sfrtc1, CASE WHEN _c40=' ' THEN null ELSE trim(_c40) END as sfrtc2, CASE WHEN _c41=' ' THEN null ELSE trim(_c41) END as sfrtc3, CASE WHEN _c42=' ' THEN null ELSE trim(_c42) END as sfrtc4, CASE WHEN _c43=' ' THEN null ELSE trim(_c43) END as sfrtc5, CASE WHEN _c44=' ' THEN null ELSE trim(_c44) END as sfcrrt, CASE WHEN _c45=' ' THEN null ELSE trim(_c45) END as sfcrcd, CASE WHEN _c46=' ' THEN null ELSE trim(_c46) END as sfcrc1, CASE WHEN _c47=' ' THEN null ELSE trim(_c47) END as sfcrc2, CASE WHEN _c48=' ' THEN null ELSE trim(_c48) END as sfcrc3, CASE WHEN _c49=' ' THEN null ELSE trim(_c49) END as sfcrc4, CASE WHEN _c50=' ' THEN null ELSE trim(_c50) END as sfcrc5, CASE WHEN _c51=' ' THEN null ELSE trim(_c51) END as sflifo, CASE WHEN _c52=' ' THEN null ELSE trim(_c52) END as sfkey1, CASE WHEN _c53=' ' THEN null ELSE trim(_c53) END as sfdatf, CASE WHEN _c54=' ' THEN null ELSE trim(_c54) END as sftatf, CASE WHEN _c55=' ' THEN null ELSE trim(_c55) END as sfuser, CASE WHEN _c56=' ' THEN null ELSE trim(_c56) END as sfinpq, CASE WHEN _c57=' ' THEN null ELSE trim(_c57) END as sfcasq, CASE WHEN _c58=' ' THEN null ELSE trim(_c58) END as sfpltq, CASE WHEN _c59=' ' THEN null ELSE trim(_c59) END as sfcolr, CASE WHEN _c60=' ' THEN null ELSE trim(_c60) END as sfdtds, CASE WHEN _c61=' ' THEN null ELSE trim(_c61) END as sftmds, CASE WHEN _c62=' ' THEN null ELSE trim(_c62) END as sfudis, CASE WHEN _c63=' ' THEN null ELSE trim(_c63) END as sfdtdu, CASE WHEN _c64=' ' THEN null ELSE trim(_c64) END as sftmdu, CASE WHEN _c65=' ' THEN null ELSE trim(_c65) END as sfudiu, CASE WHEN _c66=' ' THEN null ELSE trim(_c66) END as sfsstk, CASE WHEN _c67=' ' THEN null ELSE trim(_c67) END as sfqafl, CASE WHEN _c68=' ' THEN null ELSE trim(_c68) END as sfqadt, CASE WHEN _c69=' ' THEN null ELSE trim(_c69) END as sfqaiu, CASE WHEN _c70=' ' THEN null ELSE trim(_c70) END as sfrtvn, CASE WHEN _c71=' ' THEN null ELSE trim(_c71) END as sfrpll, CASE WHEN _c72=' ' THEN null ELSE trim(_c72) END as sforgn, CASE WHEN _c73=' ' THEN null ELSE trim(_c73) END as sfmxcd, CASE WHEN _c74=' ' THEN null ELSE trim(_c74) END as sfdofr, CASE WHEN _c75=' ' THEN null ELSE trim(_c75) END as delta from sku_master_txt")
sku_master_delta.registerTempTable("dt")
dt=sku_master_delta.orderBy("skunum")
sku_master_orc_bd=sqlContext.read.format("orc").option("header","false").option("inferSchema", "true").load("s3://aap-data-exploration/mahesh.nyalam/sku_master_26aug2021_bkp/")
sku_master_orc_bd=sku_master_orc_bd.selectExpr("_col0 as sku_rk", "_col1 as skunum", "_col2 as sfnump", "_col3 as sfnumw", "_col4 as sfdesc", "_col5 as sfdept", "_col6 as sfclas", "_col7 as sfscls", "_col8 as sfmfg1", "_col9 as sfmfg2", "_col10 as sfmfg3", "_col11 as sfvndr", "_col12 as sfdisc", "_col13 as sfhazc", "_col14 as sfcost", "_col15 as sfccs1", "_col16 as sfccs2", "_col17 as sfccs3", "_col18 as sfccs4", "_col19 as sfccs5", "_col20 as sfccdt", "_col21 as sfccd1", "_col22 as sfccd2", "_col23 as sfccd3", "_col24 as sfccd4", "_col25 as sfccd5", "_col26 as sfcorc", "_col27 as sfcccd", "_col28 as sfccc1", "_col29 as sfccc2", "_col30 as sfccc3", "_col31 as sfccc4", "_col32 as sfccc5", "_col33 as sfret", "_col34 as sfret1", "_col35 as sfret2", "_col36 as sfret3", "_col37 as sfret4", "_col38 as sfret5", "_col39 as sfretd", "_col40 as sfrtc1", "_col41 as sfrtc2", "_col42 as sfrtc3", "_col43 as sfrtc4", "_col44 as sfrtc5", "_col45 as sfcrrt", "_col46 as sfcrcd", "_col47 as sfcrc1", "_col48 as sfcrc2", "_col49 as sfcrc3", "_col50 as sfcrc4", "_col51 as sfcrc5", "_col52 as sflifo", "_col53 as sfkey1", "_col54 as sfdatf", "_col55 as sftatf", "_col56 as sfuser", "_col57 as sfinpq", "_col58 as sfcasq", "_col59 as sfpltq", "_col60 as sfcolr", "_col61 as sfdtds", "_col62 as sftmds", "_col63 as sfudis", "_col64 as sfdtdu", "_col65 as sftmdu", "_col66 as sfudiu", "_col67 as sfsstk", "_col68 as sfqafl", "_col69 as sfqadt", "_col70 as sfqaiu", "_col71 as sfrtvn", "_col72 as sfrpll", "_col73 as sforgn", "_col74 as sfmxcd", "_col75 as sfdofr", "_col76 as load_id", "_col77 as valid_start_dttm", "_col78 as valid_end_dttm", "_col79 as processed_dttm", "_col80 as delta")
sku_master_orc_bd.registerTempTable("bd")
sku_master_orc_ad=sqlContext.read.format("orc").option("header","false").option("inferSchema", "true").load("s3://aap-data-exploration/mahesh.nyalam/sku_master_27aug2021_bkp/")
sku_master_orc_ad=sku_master_orc_ad.selectExpr("_col0 as sku_rk", "_col1 as skunum", "_col2 as sfnump", "_col3 as sfnumw", "_col4 as sfdesc", "_col5 as sfdept", "_col6 as sfclas", "_col7 as sfscls", "_col8 as sfmfg1", "_col9 as sfmfg2", "_col10 as sfmfg3", "_col11 as sfvndr", "_col12 as sfdisc", "_col13 as sfhazc", "_col14 as sfcost", "_col15 as sfccs1", "_col16 as sfccs2", "_col17 as sfccs3", "_col18 as sfccs4", "_col19 as sfccs5", "_col20 as sfccdt", "_col21 as sfccd1", "_col22 as sfccd2", "_col23 as sfccd3", "_col24 as sfccd4", "_col25 as sfccd5", "_col26 as sfcorc", "_col27 as sfcccd", "_col28 as sfccc1", "_col29 as sfccc2", "_col30 as sfccc3", "_col31 as sfccc4", "_col32 as sfccc5", "_col33 as sfret", "_col34 as sfret1", "_col35 as sfret2", "_col36 as sfret3", "_col37 as sfret4", "_col38 as sfret5", "_col39 as sfretd", "_col40 as sfrtc1", "_col41 as sfrtc2", "_col42 as sfrtc3", "_col43 as sfrtc4", "_col44 as sfrtc5", "_col45 as sfcrrt", "_col46 as sfcrcd", "_col47 as sfcrc1", "_col48 as sfcrc2", "_col49 as sfcrc3", "_col50 as sfcrc4", "_col51 as sfcrc5", "_col52 as sflifo", "_col53 as sfkey1", "_col54 as sfdatf", "_col55 as sftatf", "_col56 as sfuser", "_col57 as sfinpq", "_col58 as sfcasq", "_col59 as sfpltq", "_col60 as sfcolr", "_col61 as sfdtds", "_col62 as sftmds", "_col63 as sfudis", "_col64 as sfdtdu", "_col65 as sftmdu", "_col66 as sfudiu", "_col67 as sfsstk", "_col68 as sfqafl", "_col69 as sfqadt", "_col70 as sfqaiu", "_col71 as sfrtvn", "_col72 as sfrpll", "_col73 as sforgn", "_col74 as sfmxcd", "_col75 as sfdofr", "_col76 as load_id", "_col77 as valid_start_dttm", "_col78 as valid_end_dttm", "_col79 as processed_dttm", "_col80 as delta")
sku_master_orc_ad.createOrReplaceTempView("ad")


sku_rank=spark.sql("select max(sku_rk) from bd").collect()[0][0]
start_time='2021-08-26 21:23:46.54182'
end_time='9999-12-31 00:00:00'
sku_delta_apply_1=spark.sql("select bd.sku_rk as sku_rk, bd.skunum as skunum, bd.sfnump as sfnump, bd.sfnumw as sfnumw, bd.sfdesc as sfdesc, bd.sfdept as sfdept, dt.sku_rk as sku_rki, dt.skunum as skunumi, dt.sfnump as sfnumpi, dt.sfnumw as sfnumwi, dt.sfdesc as sfdesci, dt.sfdept as sfdepti from dt left join bd on bd.skunum=dt.skunum")
sku_delta_apply_1.registerTempTable("dt_1")
sku_delta_apply_2=spark.sql("select CASE WHEN skunumi=skunum THEN sku_rk ELSE row_number() over(order by skunum,skunumi)+{} END as sku_rk,skunumi as skunum,sfnumpi as sfnump,sfnumwi as sfnumw,sfdesci as sfdesc,sfdepti as sfdept, '{}' as valid_start_dttm, '{}' as valid_end_dttm from dt_1 order by sku_rk desc".format(sku_rank,start_time,end_time))
sku_delta_apply_2.registerTempTable("dt_2")


sku_delta_apply_n=spark.sql("select bd.sku_rk, bd.skunum, bd.sfnump, bd.sfnumw, bd.sfdesc, bd.sfdept, bd.valid_start_dttm, bd.valid_end_dttm, dt_2.sku_rk as sku_rki, dt_2.skunum as skunumi,dt_2.sfnump as sfnumpi,dt_2.sfnumw as sfnumwi,dt_2.sfdesc as sfdesci,dt_2.sfdept as sfdepti, dt_2.valid_start_dttm as valid_start_dttmi, dt_2.valid_end_dttm as valid_end_dttmi from bd full outer join dt_2 on bd.skunum=dt_2.skunum and bd.sfnump=dt_2.sfnump and bd.sfnumw=dt_2.sfnumw order by sku_rki desc")
sku_delta_apply_n.registerTempTable("dt_n")
sku_delta_apply_n1=spark.sql("select dt_n.*, dt_2.skunum as skunumii from dt_n LEFT JOIN dt_2 ON dt_n.skunum=dt_2.skunum")
sku_delta_apply_n1.registerTempTable("sku_delta_apply_n1")
sku_delta_apply_n2=spark.sql("select CASE WHEN skunum is null THEN sku_rki ELSE sku_rk END as sku_rk, CASE WHEN skunum is null THEN skunumi ELSE skunum END as skunum, CASE WHEN (skunum is null) THEN sfnumpi ELSE (CASE WHEN (sfnump=sfnumpi and sfnumw=sfnumwi) THEN sfnumpi ELSE sfnump END) END as sfnump, CASE WHEN (skunum is null) THEN sfnumwi ELSE (CASE WHEN (sfnump=sfnumpi and sfnumw=sfnumwi) THEN sfnumwi ELSE sfnump END) END as sfnumw, CASE WHEN (skunum is null) THEN sfdesci ELSE (CASE WHEN (sfnump=sfnumpi and sfnumw=sfnumwi) THEN sfdesci ELSE sfdesc END) END as sfdesc, CASE WHEN (skunum is null) THEN sfdepti ELSE (CASE WHEN (sfnump=sfnumpi and sfnumw=sfnumwi) THEN sfdepti ELSE sfdept END) END as sfdept, CASE WHEN (sku_rki is not null and sku_rk is null) THEN valid_start_dttmi ELSE valid_start_dttm END as valid_start_dttm, CASE WHEN (sku_rki is not null and sku_rk is null) THEN valid_end_dttmi ELSE (CASE WHEN (sku_rki is not null) THEN valid_end_dttm ELSE (CASE WHEN (skunumii is not null and valid_end_dttm='9999-12-31 00:00:00.0') THEN valid_start_dttmi ELSE valid_end_dttm END) END) END as valid_end_dttm from sku_delta_apply_n1")
sku_delta_apply_n2.registerTempTable("sku_delta_apply_n2")

comp1=spark.sql(" select sku_rk,skunum,sfnump,sfnumw,sfdesc,sfdept,valid_start_dttm,valid_end_dttm from ad")
diff=comp1.subtract(sku_delta_apply_n2)
diff.count()

