# Databricks notebook source
# MAGIC %run ./Setup-Common

# COMMAND ----------

html_passed = f"""
<html>
<body>
<h2>Congratulations! You're all done!</h2>

While the preliminary evaluation of your project indicates that you have passed, we have a few more validation steps to run on the back-end:<br/>
<ul style="margin:0">
  <li> Code & statistical analysis of your capstone project</li>
  <li> Correlation of your account in our LMS via your email address, <b>{username}</b></li>
  <li> Final preparation of your badge
</ul>


<p>Assuming there are no issues with our last few steps, you will receive your <b>Databricks Developer Essentials Badge</b> within 2 weeks. Notification will be made by email to <b>{username}</b> regarding the availability of your digital badge via <b>Accredible</b>. 
Should we have any issues, such as not finding your email address in our LMS, we will do our best to resolve the issue using the email address provided here.
</p>

<p>Your digital badge will be available in a secure, verifiable, and digital format that you can easily retrieve via <b>Accredible</b>. You can then share your achievement via any number of different social media platforms.</p>

<p>If you have questions about the status of your badge after the initial two-week window, or if the email address listed above is incorrect, please <a href="https://help.databricks.com/s/contact-us?ReqType=training" target="_blank">submit a ticket</a> with the subject "Core Capstone" and your Registration ID (<b>{registration_id}</b>) in the message body. Please allow us 3-5 business days to respond.</p>

One final note: In order to comply with <a href="https://oag.ca.gov/privacy/ccpa" target="_blank">CCPA</a> and <a href="https://gdpr.eu/" target="_blank">GDPR</a>, which regulate the collection of your personal information, the status of this capstone and its correlation to your email address will be deleted within 30 days of its submission.
</body>
</html>
"""

html_failed = f"""
<html>
<body>
<h2>Almost There!</h2>

<p>Our preliminary evaluation of your project indicates that you have not passed.</p>

<p>In order for your project to be submitted <b>all</b> reality checks in all exercises must pass.</p>

<p>In some cases this problem can be resolved by simply clearning a notebook's state (<b>Clear State & Results</b>) and then selecting <b>Run All</b> from the toolbar above.</p>

<p>If your project continues to fail validation, please review each step in the coresponding exercise to ensure that you are have properly addressed each requirement.</p>
</body>
</html>
"""

# Setup Final
def submit_capstone():

  suite_name = "ex.07.all"
  suite = TestSuite()

  suite.test(f"{suite_name}.reg_id", f"Valid Registration ID", testFunction = lambda: validate_registration_id(registration_id), dependsOn=[suite.lastTestId()])
  
  for i in range(1,7):
    try:
      path = f"{working_dir}/raw/_meta/exercise_{i}_status.json"
      passed = spark.read.json(path).first()["passed"]
    except:
      passed = False
      
    suite.testEquals(f"{suite_name}.{i}", f"Completed Exercise #{i}", passed, True, dependsOn=[suite.lastTestId()])
    
  suite.testEquals(f"{suite_name}.7", f"Completed Exercise #7", True, True, dependsOn=[suite.lastTestId()])

  daLogger.logTestSuite(suite_name, registration_id, suite)
  daLogger.logAggregation(getNotebookName(), registration_id, TestResultsAggregator)

  suite.displayResults()

  if suite.passed and TestResultsAggregator.passed:
    displayHTML(html_passed)
    daLogger.logCompletion(registration_id, username, 7)
  else:
    displayHTML(html_failed)

None

# COMMAND ----------

html = html_intro()
html += html_header()

html += html_row_fun("submit_capstone()", "A utility function for checking the final state of your project.")

html += "</table></body></html>"

displayHTML(html)
