task echo_hello_world {
  command {
     gcloud config list account --format "value(core.account)"

     # Check that the NIO files were not localized
     INSTANCE=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/name" -H "Metadata-Flavor: Google")
     TOKEN=$(gcloud auth application-default print-access-token)
     curl "https://www.googleapis.com/compute/v1/projects/broad-dsde-cromwell-dev/zones/us-central1-c/instances/$INSTANCE" -H "Authorization: Bearer $TOKEN" -H 'Accept: application/json'
  }

  runtime {
    docker: "google/cloud-sdk:slim"
  }

  output {
    String account = read_string(stdout())
  }
}

workflow run_on_vpc {
  call echo_hello_world

  output {
    String account_used = echo_hello_world.account
  }
}
