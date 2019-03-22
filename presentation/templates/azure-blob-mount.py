# Databricks notebook source
# To remove the widgets and reset the values
dbutils.widgets.remove("accountName")
dbutils.widgets.remove("accessKey")
dbutils.widgets.remove("blobName")

# COMMAND ----------

dbutils.widgets.text("accountName", "No Account Specified", label = "Account Name")
dbutils.widgets.text("accessKey", "No Access Key Specified", label = "Access Key")
dbutils.widgets.text("blobName", "No Blob Specified", label = "Blob Container")

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/"+getArgument("blobName"))

# COMMAND ----------

# Only run once

accountname = getArgument("accountName")
accountkey = getArgument("accessKey")

fullname = "fs.azure.account.key." +accountname+ ".blob.core.windows.net"
accountsource = "wasbs://"+ getArgument("blobName") + "@" +accountname+ ".blob.core.windows.net"

dbutils.fs.mount(
  source = accountsource
  , mount_point = "/mnt/"+ getArgument("blobName")
  , extra_configs = {fullname : accountkey}
)

# COMMAND ----------

# Only run once
# to unmount a blob execute the following command:
dbutils.fs.unmount("/mnt/" + getArgument("blobName"))