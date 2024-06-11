# Learn Terraform - Provision an EKS Cluster

This repo is a companion repo to the [Provision an EKS Cluster tutorial](https://developer.hashicorp.com/terraform/tutorials/kubernetes/eks), containing
Terraform configuration files to provision an EKS cluster on AWS.

# Project instructions

aplicar as configurações do Terraform

terraform init
terraform apply

Run the following command to retrieve the access credentials for your cluster and configure kubectl.

aws eks --region $(terraform output -raw region) update-kubeconfig --name $(terraform output -raw cluster_name)

You can now use kubectl to manage your cluster and deploy Kubernetes configurations to it.

kubectl apply -f kubernetes