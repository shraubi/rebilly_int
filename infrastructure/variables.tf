variable project_id {
  type        = string
  default     = "big-query-database-376613"
  description = "Project ID, used to enforce providing a project id"
}

variable region {
  type        = string
  default     = "europe-west3"
  description = "Project region"
}

variable sa_composer {
  type        = string
  default     = "sa-composer@big-query-database-376613.iam.gserviceaccount.com"
  description = "Service account for environment creation"
}