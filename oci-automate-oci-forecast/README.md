### Introduction

The customer expects to run OCI Forecasting service for their ticket volume forecasting every week. Customer has already given the primary dataset (dataset of last 5 years tickets) and has also provided an API, which gives results for last 7 days' ticket volume for each series. Customer expects before running the service after 7 days, we append the last 7 days of data in the historical dataset and add '0' in missing dates. As per the customer, there can be some missing dates in the last 7 days' record set, when there was no ticket in a particular series (support queue) on that day.


### End-to-End Flow
![pipeline.drawio(2).png](..%2F..%2F..%2F..%2F..%2F..%2FDownloads%2Fpipeline.drawio%282%29.png)

### Code Flow
![flow_oci_function.drawio(1).png](..%2F..%2F..%2F..%2F..%2F..%2FDownloads%2Fflow_oci_function.drawio%281%29.png)