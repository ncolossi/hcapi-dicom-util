#
# Commands to deploy import/export adapter for DICOM
# Run commands manually, after validating each yaml file
#

# AET dictionary
kubectl create configmap aet-dictionary --from-file=AETs.json

# Import adapter deploy
kubectl apply -f dicom_adapter_import.yaml
# kubectl describe deployment dicom-adapter-import
# kubectl get pods -l app=dicom-adapter-import
# kubectl describe pod dicom-adapter-import-xxx

# LB for Import adapter deploy
kubectl apply -f dicom_adapter_load_balancer.yaml
kubectl describe service dicom-adapter-import-load-balancer     # LoadBalancer Ingress IP 

# Export adapter deploy
kubectl apply -f dicom_adapter_export.yaml
# kubectl describe deployment dicom-adapter-import
# kubectl get pods -l app=dicom-adapter-import
# kubectl describe pod dicom-adapter-import-xxx
