# Project Overview
This project will introduce you to the core concepts of Apache Airflow. To complete the project, you will need to create your own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

We have provided you with a project template that takes care of all the imports and provides four empty operators that need to be implemented into functional pieces of a data pipeline. The template also contains a set of tasks that need to be linked to achieve a coherent and sensible data flow within the pipeline.

You'll be provided with a helpers class that contains all the SQL transformations. Thus, you won't need to write the ETL yourselves, but you'll need to execute it with your custom operators.

![Example Dag](/images/example-dag.png)

# Add Airflow Connections
Here, we'll use Airflow's UI to configure your AWS credentials and connection to Redshift.

1. To go to the Airflow UI:
    * You can use the Project Workspace here and click on the blue **Access Airflow** button in the bottom right.
    * If you'd prefer to run Airflow locally, open http://localhost:8080 in Google Chrome (other browsers occasionally have issues rendering the Airflow UI).

2. Click on the **Admin** tab and select **Connections**.

![Admin Connections](/images/admin-connections.png)

3. Under **Connections**, select **Create**.

![Create Connection](/images/create-connection.png)

4. On the create connection page, enter the following values:

    * **Conn Id:** Enter `aws_credentials`.  
    * **Conn Type:** Enter `Amazon Web Services`.  
    * **Login:** Enter your Access key ID from the IAM User credentials you downloaded earlier.  
    * **Password:** Enter your Secret access key from the IAM User credentials you downloaded earlier.  

Once you've entered these values, select **Save and Add Another.**

![Connection AWS Credentials](/images/connection-aws-credentials.png)

5. On the next create connection page, enter the following values:

    * **Conn Id:** Enter `redshift`.  
    * **Conn Type:** Enter `Postgres`.  
    * **Host:** Enter the endpoint of your Redshift cluster, excluding the port at the end. You can find this by selecting your cluster in the **Clusters** page of the Amazon Redshift console. See where this is located in the screenshot below. IMPORTANT: Make sure to **NOT** include the port at the end of the Redshift endpoint string.  
    * **Schema:** Enter `dev`. This is the Redshift database you want to connect to.  
    * **Login:** Enter `awsuser`.  
    * **Password:** Enter the password you created when launching your Redshift cluster.  
    * **Port:** Enter `5439`.  

Once you've entered these values, select **Save**.  

![Cluster Details](/images/cluster-details.png)

![Connect Redshift](/images/connection-redshift.png)

Awesome! You're now all configured to run Airflow with Redshift.

**WARNING: Remember to DELETE your cluster each time you are finished working to avoid large, unexpected costs.**
