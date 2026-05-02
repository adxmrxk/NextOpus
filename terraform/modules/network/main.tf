################################################################################
# NextOpus - Network Module
# Creates VCN, Subnets, Internet Gateway, NAT Gateway, and Route Tables
################################################################################

# ==============================================================================
# Virtual Cloud Network (VCN)
# ==============================================================================

resource "oci_core_vcn" "main" {
  compartment_id = var.compartment_ocid
  cidr_blocks    = [var.vcn_cidr]
  display_name   = "${var.project_name}-vcn"
  dns_label      = replace(var.project_name, "-", "")

  freeform_tags = var.freeform_tags
}

# ==============================================================================
# Internet Gateway (for public subnet outbound traffic)
# ==============================================================================

resource "oci_core_internet_gateway" "main" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.main.id
  display_name   = "${var.project_name}-igw"
  enabled        = true

  freeform_tags = var.freeform_tags
}

# ==============================================================================
# NAT Gateway (for private subnet outbound traffic)
# Note: NAT Gateway is NOT part of Always Free Tier - using public subnet only
# Uncomment if you have paid resources
# ==============================================================================

# resource "oci_core_nat_gateway" "main" {
#   compartment_id = var.compartment_ocid
#   vcn_id         = oci_core_vcn.main.id
#   display_name   = "${var.project_name}-nat"
#
#   freeform_tags = var.freeform_tags
# }

# ==============================================================================
# Route Tables
# ==============================================================================

# Public Route Table - routes to Internet Gateway
resource "oci_core_route_table" "public" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.main.id
  display_name   = "${var.project_name}-public-rt"

  route_rules {
    network_entity_id = oci_core_internet_gateway.main.id
    destination       = "0.0.0.0/0"
    destination_type  = "CIDR_BLOCK"
    description       = "Route to Internet via IGW"
  }

  freeform_tags = var.freeform_tags
}

# Private Route Table - internal only (no NAT in free tier)
resource "oci_core_route_table" "private" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.main.id
  display_name   = "${var.project_name}-private-rt"

  # No routes - private subnet is isolated
  # Add NAT Gateway route here if using paid tier

  freeform_tags = var.freeform_tags
}

# ==============================================================================
# DHCP Options
# ==============================================================================

resource "oci_core_dhcp_options" "main" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.main.id
  display_name   = "${var.project_name}-dhcp"

  options {
    type        = "DomainNameServer"
    server_type = "VcnLocalPlusInternet"
  }

  options {
    type                = "SearchDomain"
    search_domain_names = ["${replace(var.project_name, "-", "")}.oraclevcn.com"]
  }

  freeform_tags = var.freeform_tags
}

# ==============================================================================
# Subnets
# ==============================================================================

# Public Subnet - K3s nodes will be placed here for Always Free Tier
resource "oci_core_subnet" "public" {
  compartment_id             = var.compartment_ocid
  vcn_id                     = oci_core_vcn.main.id
  cidr_block                 = var.public_subnet_cidr
  display_name               = "${var.project_name}-public-subnet"
  dns_label                  = "public"
  route_table_id             = oci_core_route_table.public.id
  dhcp_options_id            = oci_core_dhcp_options.main.id
  security_list_ids          = var.public_security_list_ids
  prohibit_public_ip_on_vnic = false
  prohibit_internet_ingress  = false

  freeform_tags = var.freeform_tags
}

# Private Subnet - for future database/internal services
resource "oci_core_subnet" "private" {
  compartment_id             = var.compartment_ocid
  vcn_id                     = oci_core_vcn.main.id
  cidr_block                 = var.private_subnet_cidr
  display_name               = "${var.project_name}-private-subnet"
  dns_label                  = "private"
  route_table_id             = oci_core_route_table.private.id
  dhcp_options_id            = oci_core_dhcp_options.main.id
  security_list_ids          = var.private_security_list_ids
  prohibit_public_ip_on_vnic = true
  prohibit_internet_ingress  = true

  freeform_tags = var.freeform_tags
}
