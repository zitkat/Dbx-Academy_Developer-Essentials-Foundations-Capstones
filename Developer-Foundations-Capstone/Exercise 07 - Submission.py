# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Exercise #7 - Project Submission
# MAGIC 
# MAGIC * Congrats on getting to the end of the capstone! 
# MAGIC * With each execution of each reality check, our tracking system records your progress.
# MAGIC * This does not ensure that you have passed; why?
# MAGIC   * Notebooks executed out of order alter the datasets on which subsequent exercises depend on.
# MAGIC   * Within any given exercise, the out-of-order-execution of cells presents a similar problem. For example, it is possible to report that Reality-Check-3 passed and **then** Reality-Check-1 passed.
# MAGIC   * In both cases, our systems recorded the out-of-order-execution of the exercises and the individual cells (namely the Reality Checks), which is processed as a failed execution.
# MAGIC * For the capstone project to be correctly evaluated, you must:
# MAGIC   * Run each exercise in order, from top to bottom.
# MAGIC   * Start with Exercise #1 and proceed through Exercise #7.
# MAGIC   * Within each exercise, select **Clear**|**Clear State & Results** and then **Run All**
# MAGIC   * If the execution of any exercise fails and you have to make corrections, please make sure to re-run the entire exercise and all subsequent exercises.

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Setup Exercise #7</h2>
# MAGIC 
# MAGIC To get started, run the following cell to setup this exercise, declaring exercise-specific variables and functions.

# COMMAND ----------

# MAGIC %run ./_includes/Setup-Exercise-07

# COMMAND ----------

# MAGIC %md Run the following cell to submit your capstone project:

# COMMAND ----------

submit_capstone()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>