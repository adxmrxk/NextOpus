################################################################################
# NextOpus - Security Module
# Security Lists and Network Security Groups for K3s Cluster
################################################################################

# ==============================================================================
# Public Subnet Security List
# Allows SSH, HTTP/HTTPS, K3s API, and NodePort range
# ==============================================================================

resource "oci_core_security_list" "public" {
  compartment_id = var.compartment_ocid
  vcn_id         = var.vcn_id
  display_name   = "${var.project_name}-public-sl"

  # --------------------------------------------------------------------------
  # Egress Rules - Allow all outbound traffic
  # --------------------------------------------------------------------------
  egress_security_rules {
    destination      = "0.0.0.0/0"
    protocol         = "all"
    destination_type = "CIDR_BLOCK"
    description      = "Allow all outbound traffic"
  }

  # --------------------------------------------------------------------------
  # Ingress Rules
  # --------------------------------------------------------------------------

  # SSH Access (restrict in production)
  ingress_security_rules {
    protocol    = "6" # TCP
    source      = var.allowed_ssh_cidr
    source_type = "CIDR_BLOCK"
    description = "SSH access"

    tcp_options {
      min = 22
      max = 22
    }
  }

  # HTTP
  ingress_security_rules {
    protocol    = "6"
    source      = "0.0.0.0/0"
    source_type = "CIDR_BLOCK"
    description = "HTTP traffic"

    tcp_options {
      min = 80
      max = 80
    }
  }

  # HTTPS
  ingress_security_rules {
    protocol    = "6"
    source      = "0.0.0.0/0"
    source_type = "CIDR_BLOCK"
    description = "HTTPS traffic"

    tcp_options {
      min = 443
      max = 443
    }
  }

  # K3s API Server
  ingress_security_rules {
    protocol    = "6"
    source      = var.allowed_ssh_cidr
    source_type = "CIDR_BLOCK"
    description = "K3s API server"

    tcp_options {
      min = 6443
      max = 6443
    }
  }

  # Kubernetes NodePort range
  ingress_security_rules {
    protocol    = "6"
    source      = "0.0.0.0/0"
    source_type = "CIDR_BLOCK"
    description = "Kubernetes NodePort services"

    tcp_options {
      min = 30000
      max = 32767
    }
  }

  # ICMP - Path MTU Discovery
  ingress_security_rules {
    protocol    = "1" # ICMP
    source      = "0.0.0.0/0"
    source_type = "CIDR_BLOCK"
    description = "ICMP Path MTU Discovery"

    icmp_options {
      type = 3
      code = 4
    }
  }

  # ICMP - Ping from VCN
  ingress_security_rules {
    protocol    = "1"
    source      = var.vcn_cidr
    source_type = "CIDR_BLOCK"
    description = "ICMP ping from VCN"

    icmp_options {
      type = 8
    }
  }

  freeform_tags = var.freeform_tags
}

# ==============================================================================
# Private Subnet Security List
# Internal services only
# ==============================================================================

resource "oci_core_security_list" "private" {
  compartment_id = var.compartment_ocid
  vcn_id         = var.vcn_id
  display_name   = "${var.project_name}-private-sl"

  # Egress - Allow all outbound within VCN
  egress_security_rules {
    destination      = var.vcn_cidr
    protocol         = "all"
    destination_type = "CIDR_BLOCK"
    description      = "Allow all traffic within VCN"
  }

  # Ingress - Allow all from VCN
  ingress_security_rules {
    protocol    = "all"
    source      = var.vcn_cidr
    source_type = "CIDR_BLOCK"
    description = "Allow all traffic from VCN"
  }

  freeform_tags = var.freeform_tags
}

# ==============================================================================
# Network Security Group for K3s Cluster
# More granular security rules for inter-node communication
# ==============================================================================

resource "oci_core_network_security_group" "k3s" {
  compartment_id = var.compartment_ocid
  vcn_id         = var.vcn_id
  display_name   = "${var.project_name}-k3s-nsg"

  freeform_tags = var.freeform_tags
}

# --------------------------------------------------------------------------
# K3s Inter-node Communication Rules
# --------------------------------------------------------------------------

# Flannel VXLAN (UDP 8472)
resource "oci_core_network_security_group_security_rule" "k3s_flannel_vxlan" {
  network_security_group_id = oci_core_network_security_group.k3s.id
  direction                 = "INGRESS"
  protocol                  = "17" # UDP
  source                    = var.vcn_cidr
  source_type               = "CIDR_BLOCK"
  description               = "Flannel VXLAN overlay network"

  udp_options {
    destination_port_range {
      min = 8472
      max = 8472
    }
  }
}

# Kubelet API (TCP 10250)
resource "oci_core_network_security_group_security_rule" "k3s_kubelet" {
  network_security_group_id = oci_core_network_security_group.k3s.id
  direction                 = "INGRESS"
  protocol                  = "6" # TCP
  source                    = var.vcn_cidr
  source_type               = "CIDR_BLOCK"
  description               = "Kubelet API"

  tcp_options {
    destination_port_range {
      min = 10250
      max = 10250
    }
  }
}

# K3s Supervisor API (TCP 9345)
resource "oci_core_network_security_group_security_rule" "k3s_supervisor" {
  network_security_group_id = oci_core_network_security_group.k3s.id
  direction                 = "INGRESS"
  protocol                  = "6"
  source                    = var.vcn_cidr
  source_type               = "CIDR_BLOCK"
  description               = "K3s supervisor API for node registration"

  tcp_options {
    destination_port_range {
      min = 9345
      max = 9345
    }
  }
}

# etcd clients (TCP 2379-2380) - for HA setups
resource "oci_core_network_security_group_security_rule" "k3s_etcd" {
  network_security_group_id = oci_core_network_security_group.k3s.id
  direction                 = "INGRESS"
  protocol                  = "6"
  source                    = var.vcn_cidr
  source_type               = "CIDR_BLOCK"
  description               = "etcd client and peer communication"

  tcp_options {
    destination_port_range {
      min = 2379
      max = 2380
    }
  }
}

# Metrics Server (TCP 4443)
resource "oci_core_network_security_group_security_rule" "k3s_metrics" {
  network_security_group_id = oci_core_network_security_group.k3s.id
  direction                 = "INGRESS"
  protocol                  = "6"
  source                    = var.vcn_cidr
  source_type               = "CIDR_BLOCK"
  description               = "Kubernetes metrics server"

  tcp_options {
    destination_port_range {
      min = 4443
      max = 4443
    }
  }
}

# Istio ports (if using service mesh)
resource "oci_core_network_security_group_security_rule" "k3s_istio_envoy" {
  network_security_group_id = oci_core_network_security_group.k3s.id
  direction                 = "INGRESS"
  protocol                  = "6"
  source                    = var.vcn_cidr
  source_type               = "CIDR_BLOCK"
  description               = "Istio Envoy sidecar proxy"

  tcp_options {
    destination_port_range {
      min = 15000
      max = 15021
    }
  }
}

# Allow all egress from NSG
resource "oci_core_network_security_group_security_rule" "k3s_egress_all" {
  network_security_group_id = oci_core_network_security_group.k3s.id
  direction                 = "EGRESS"
  protocol                  = "all"
  destination               = "0.0.0.0/0"
  destination_type          = "CIDR_BLOCK"
  description               = "Allow all outbound traffic"
}
