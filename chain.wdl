workflow wf_chain {
  File firstInput
  call stepA { input: in=firstInput }
  call stepB { input: in=stepA.out }
  call stepC { input: in=stepB.out }
}
task stepA {
  File in
  command { echo "stepA" > outputA.txt }
  output { File out = "outputA.txt" }
  runtime {
    docker: "ubuntu:latest"
  }
}
task stepB {
  File in
  command { echo ${in} "stepB" > outputB.txt }
  output { File out = "outputB.txt" }
  runtime {
    docker: "ubuntu:latest"
  }
}
task stepC {
  File in
  command { echo ${in} "stepC" > outputC.txt }
  output { File out = "outputC.txt" }
  runtime {
    docker: "ubuntu:latest"
  }
}
