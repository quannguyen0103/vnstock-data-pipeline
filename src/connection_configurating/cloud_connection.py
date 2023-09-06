from airflow import models, settings

# Create the gcp connection
cloud_conn = models.Connection(
    conn_id="gcp_connection",
    conn_type="google_cloud_platform",
    extra={"extra__google_cloud_platform__project": "project_id",
          "extra__google_cloud_platform__key_path": "/home/user/keyfile.json"}
)

# Add the connection to Airflow
session = settings.Session()
session.add(cloud_conn)
session.commit()
session.close()
