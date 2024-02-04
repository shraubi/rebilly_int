provider "google" {
  region = var.region
  project = var.project_id
  credentials = file("big-query-database-376613-5baee0c52843.json")
}

resource "google_project_service" "head" {
  project = var.project_id
  service = "iam.googleapis.com"
  disable_dependent_services=false
}

resource "google_project_iam_custom_role" "custom_roles" {
  for_each = local.custom_roles

  project     = var.project_id
  role_id     = each.value["roleId"]
  title       = each.value["title"]
  description = each.value["description"]
  permissions = each.value["permissions"]
  depends_on = [google_project_service.head]

}

resource "google_service_account" "sa_list" {
  project  = var.project_id
  for_each = local.service_accounts
  account_id   = each.value["account_id"]
  display_name = each.value["display_name"]
  depends_on = [google_project_service.head]
}

resource "google_project_iam_member" "sa_roles" {
  for_each   = {for idx, sa in local.sa_roles_flattened : "${sa["account_id"]}_${sa["role"]}" => sa}
  project    = var.project_id
  role       = replace(each.value["role"], "big-query-database-376613", var.project_id)
  member     = "serviceAccount:${each.value["account_id"]}@big-query-database-376613.iam.gserviceaccount.com"
  depends_on = [google_service_account.sa_list]
}

resource "google_service_account_iam_member" "admin_account_iam" {
  for_each           = local.service_accounts
  service_account_id = "projects/big-query-database-376613/serviceAccounts/${each.value["account_id"]}@big-query-database-376613.iam.gserviceaccount.com"
  role               = each.value["owner_role"]
  member             = each.value["owner_email"]
  depends_on         = [google_service_account.sa_list]
}

# Enables API in new project
# resource "google_project_service" "composer_api" {
#   provider = google
#   project = var.project_id
#   service = "composer.googleapis.com"
#   // Disabling Cloud Composer API might irreversibly break all other
#   // environments in your project.
#   disable_on_destroy = false
# }

resource "google_compute_network" "main" {
  name                    = "composer-network"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "sub" {
  name          = "composer-subnetwork"
  ip_cidr_range = "10.2.0.0/16"
  region        = var.region
  network       = google_compute_network.main.id
}

resource "google_composer_environment" "tf_environment" {
  provider = google
  project = var.project_id
  name = "composerdev"
  config {
    node_config {
      subnetwork = google_compute_subnetwork.sub.id
      network = google_compute_network.main.id
      service_account = var.sa_composer
    }
  }
}