variable "compartment_ocid" {
  description = "Target compartment OCID."
  type        = string
}

variable "subnet_ocid" {
  description = "Target subnet OCID (regional subnet recommended)."
  type        = string
}

variable "region" {
  description = "Region"
  type        = string
}

variable "instance_count" {
  description = "How many instances to create."
  type        = number
  default     = 4
}

variable "shape" {
  description = "Compute shape (e.g., VM.Standard3.Flex)."
  type        = string
}

variable "ocpus" {
  description = "For Flex shapes: OCPUs."
  type        = number
  default     = 2
}

variable "memory_gbs" {
  description = "For Flex shapes: Memory (GB)."
  type        = number
  default     = 16
}

variable "image_ocid" {
  description = "Custom Image OCID."
  type        = string
}

variable "ssh_public_key_path" {
  description = "Path to your public SSH key (e.g., ~/.ssh/id_rsa.pub)."
  type        = string
}

variable "assign_public_ip" {
  description = "Assign public IPs to instances."
  type        = bool
  default     = true
}

variable "display_name_prefix" {
  description = "Display name prefix for instances."
  type        = string
  default     = "tigris-vm"
}

// variable "local_folder" {
//   description = "Local folder to upload to each VM."
//   type        = string
// }

variable "remote_folder_path" {
  description = "Where to unpack the folder on the VM."
  type        = string
  default     = "/opt/payload"
}

variable "bucket_name" {
  description = "Object Storage bucket name to stage the archive."
  type        = string
  default     = "vm-payload-bucket"
}

variable "par_valid_days" {
  description = "Pre-Authenticated Request validity in days."
  type        = number
  default     = 7
}
