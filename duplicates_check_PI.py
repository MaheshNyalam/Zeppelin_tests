df_cq_pi = sqlContext.sql("""select store_number, 
                                    sku_number, 
                                    merchandise_group_desc 
                                from dpdm_prod.cq_prediction_input""")
df_cq_pi.createOrReplaceTempView("df_cq_pi")
df_dupes_check = sqlContext.sql("""select store_number, 
                                          sku_number, 
                                          merchandise_group_desc, 
                                          count(*) 
                                    from df_cq_pi 
                                    where store_number is not null 
                                        and sku_number is not null 
                                    group by store_number, sku_number, merchandise_group_desc 
                                    having count(*) > 1""")
df_dupes_check.createOrReplaceTempView("df_dupes_check")
df_dupes_bpgs = sqlContext.sql("""select distinct merchandise_group_desc 
                        from df_dupes_check 
                        order by merchandise_group_desc""")
print "No.of BPG's having duplicates: ", df_dupes_bpgs.count()
df_dupes_bpgs.show(1000, truncate=False)