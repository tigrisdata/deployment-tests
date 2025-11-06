compartment_ocid     = "ocid1.compartment.oc1..."
subnet_ocid          = "ocid1.subnet.oc1.<region>...."
region               = "region"

shape                = "VM.Standard2.4"
ocpus                = 4
memory_gbs           = 60

image_ocid           = "ocid1.image.oc1.<region>...."   # your Custom Image OCID
ssh_public_key_path  = "~/.ssh/<public key>"

instance_count       = 4
assign_public_ip     = true

#local_folder         = "./my-folder-to-upload"
#remote_folder_path   = "/opt/payload"

#bucket_name          = "vm-payload-bucket"
#par_valid_days       = 7
