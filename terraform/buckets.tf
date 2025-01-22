resource "google_storage_bucket" "bucket_etl" {
  name          = var.storage_etl
  project       = var.project_id
  location      = var.region
  force_destroy = true

  storage_class            = "STANDARD"
  public_access_prevention = "enforced"

  soft_delete_policy {
    retention_duration_seconds = 0
  }
}

resource "google_storage_bucket_object" "envio" {
  for_each = toset(var.files_buckets)

  name   = each.value
  bucket = google_storage_bucket.bucket_etl.name
  source = "../${each.value}"
}