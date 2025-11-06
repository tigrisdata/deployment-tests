# # Bucket to stage the payload
# resource "oci_objectstorage_bucket" "bucket" {
#   compartment_id = var.compartment_ocid
#   namespace      = data.oci_objectstorage_namespace.ns.namespace
#   name           = var.bucket_name
#   access_type    = "NoPublicAccess"
#   auto_tiering   = "Disabled"
# }

# # Upload the archive to the bucket
# resource "oci_objectstorage_object" "payload" {
#   namespace         = data.oci_objectstorage_namespace.ns.namespace
#   bucket            = oci_objectstorage_bucket.bucket.name
#   object            = local.payload_name
#   content_type      = "application/gzip"
#   source            = data.archive_file.folder_tgz.output_path
#   content_language  = "en"
#   content_disposition = "attachment"
# }

# # Create a Pre-Authenticated Request (PAR) to the object so VMs can download without creds
# resource "oci_objectstorage_preauthrequest" "payload_par" {
#   namespace     = data.oci_objectstorage_namespace.ns.namespace
#   bucket        = oci_objectstorage_bucket.bucket.name
#   name          = "payload-par"
#   access_type   = "ObjectRead"
#   time_expires  = timeadd(timestamp(), "${var.par_valid_days}d")
#   object_name   = oci_objectstorage_object.payload.object
# }

# # Public URL for the PAR (constructed)
# locals {
#   payload_par_url = "https://objectstorage.${var.region}.oraclecloud.com${oci_objectstorage_preauthrequest.payload_par.access_uri}"
# }

# # Region variable for URL construction (usually from provider env, but explicit here)
# variable "region" {
#   description = "OCI region identifier (e.g., us-ashburn-1)."
#   type        = string
# }
