resource "google_service_account" "conta_servico" {
  account_id  = "${var.template_name}-${var.project_id}"
  project     = var.project_id
  description = "conta de servico para o projeto"
}

resource "google_project_iam_custom_role" "conta_fluxo" {
  role_id     = "workflowProcPapeis"
  title       = "workflow-proc-papeis"
  description = "Papeis para executar um workflow dataproc template"
  permissions = ["dataproc.workflowTemplates.instantiate", "iam.serviceAccounts.actAs"]
}

resource "google_project_iam_member" "accesso_work" {
  project = var.project_id
  member  = "serviceAccount:${google_service_account.conta_servico.email}"
  role    = "projects/${var.project_id}/roles/${google_project_iam_custom_role.conta_fluxo.role_id}"
}