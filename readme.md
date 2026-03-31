This will use some interesting libs like spark and jax. 

To install Java on Mac for spark usage you need the following commands: 

- brew install openjdk@17
- sudo ln -sfn $(brew --prefix)/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk

Apparently version 17 is the best for spark for now. 