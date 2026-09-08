################################################################################
# NextOpus - Network Module Provider Requirements
# Child modules must pin the provider source explicitly; without this Terraform
# resolves "oci_*" resources to the legacy hashicorp/oci namespace, which the
# root provider block does not configure.
################################################################################

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    oci = {
      source  = "oracle/oci"
      version = ">= 5.0.0"
    }
  }
}
