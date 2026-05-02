################################################################################
# NextOpus - Terraform Provider Configuration
# Oracle Cloud Infrastructure (OCI) - Always Free Tier
################################################################################

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    oci = {
      source  = "oracle/oci"
      version = ">= 5.0.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = ">= 4.0.0"
    }
    local = {
      source  = "hashicorp/local"
      version = ">= 2.4.0"
    }
  }

  # Uncomment to use remote state (recommended for production)
  # backend "s3" {
  #   bucket                      = "nextopus-tfstate"
  #   key                         = "infrastructure/terraform.tfstate"
  #   region                      = "us-ashburn-1"
  #   endpoint                    = "https://<namespace>.compat.objectstorage.<region>.oraclecloud.com"
  #   skip_region_validation      = true
  #   skip_credentials_validation = true
  #   skip_metadata_api_check     = true
  #   force_path_style            = true
  # }
}

provider "oci" {
  tenancy_ocid     = var.tenancy_ocid
  user_ocid        = var.user_ocid
  fingerprint      = var.fingerprint
  private_key_path = var.private_key_path
  region           = var.region
}

provider "tls" {}
provider "local" {}
