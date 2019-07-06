workflow wf_chain {
  File firstInput
  File firstInput2
  call stepa { input: in=firstInput, in2=firstInput2 }
  call stepb { input: in=stepa.out }
  call stepc { input: in=stepb.out }
}
task stepa {
  File in
  File in2
  command { cat ${in} > outputa.txt }
  output { File out = "outputa.txt" }
  runtime {
    docker: "ubuntu:latest"
  }
}
task stepb {
  File in
  command { cat ${in} > outputb.txt }
  output { File out = "outputb.txt" }
  runtime {
    docker: "ubuntu:latest"
  }
}
task stepc {
  File in
  command { cat ${in} > outputc.txt }
  output { File out = "outputc.txt" }
  runtime {
    docker: "ubuntu:latest"
  }
}
