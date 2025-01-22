resource "google_cloud_scheduler_job" "jos_work" {
  name        = "sched-${var.template_name}"
  project     = var.project_id
  region      = var.region
  description = "executa spark job"
  schedule    = "0 7 * * 1-5"
  time_zone   = "America/Fortaleza"

  http_target {
    http_method = "POST"
    uri         = "https://dataproc.googleapis.com/v1/projects/${var.project_id}/regions/${var.region}/workflowTemplates/${var.template_name}:instantiate?alt=json"

    oauth_token {
      service_account_email = google_service_account.conta_servico.email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }

  depends_on = [
    google_dataproc_workflow_template.template,
    google_service_account.conta_servico
  ]
}