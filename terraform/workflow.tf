resource "google_dataproc_workflow_template" "template" {
  name     = var.template_name
  location = var.region

  placement {
    managed_cluster {
      cluster_name = var.name_cluster

      config {
        gce_cluster_config {
          subnetwork = "default"
          zone       = "${var.region}-b"
          metadata = {
            PIP_PACKAGES = "athena-mvsh==0.0.14 python-dotenv==1.0.1"
          }
        }
        master_config {
          num_instances = 1
          machine_type  = var.machine_type
          disk_config {
            boot_disk_type    = "pd-balanced"
            boot_disk_size_gb = 100
          }
        }
        software_config {
          image_version       = "2.2-debian12"
          optional_components = ["JUPYTER"]
          properties = {
            "dataproc:dataproc.allow.zero.workers" : "true"
          }
        }
        initialization_actions {
          executable_file = "gs://${var.storage_etl}/${var.install_pip_sh}"
        }
      }
    }
  }

  jobs {
    step_id = var.step_id
    pyspark_job {
      main_python_file_uri = "gs://${var.storage_etl}/${var.main_python_job}"
      file_uris            = [for name in var.files_urls : "gs://${var.storage_etl}/${name}"]
      python_file_uris     = [for name in var.files_urls_python : "gs://${var.storage_etl}/${name}"]
    }
  }
}