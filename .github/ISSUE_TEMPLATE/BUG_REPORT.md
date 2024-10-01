---
name: Bug Report 🐞
about: Something isn't working as expected? Here is the right place to report.
labels: bug
---


<!--
If you need urgent assistance then file the issue using the support process: 
https://community.snowflake.com/s/article/How-To-Submit-a-Support-Case-in-Snowflake-Lodge 
otherwise continue here. 
-->


Please answer these questions before submitting your issue. 
In order to accurately debug the issue this information is required. Thanks!

1. What version of GO driver are you using?

   
2. What operating system and processor architecture are you using?

   
3. What version of GO are you using?
run `go version` in your console

4.Server version:* E.g. 1.90.1
You may get the server version by running a query:
```
SELECT CURRENT_VERSION();
```
5. What did you do?

   If possible, provide a recipe for reproducing the error.
   A complete runnable program is good.

6. What did you expect to see?

   What should have happened and what happened instead?

7. Can you set logging to DEBUG and collect the logs?

   https://community.snowflake.com/s/article/How-to-generate-log-file-on-Snowflake-connectors
   
   Before sharing any information, please be sure to review the log and remove any sensitive
   information.
