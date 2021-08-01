# apachebeam-dataflow

Information about the Pipeline
------------------------------
The dataflow job can read avro files from 2 sources, called Employee data and Transaction data. Employee data has id,name,company and salary.Transaction data has id and transactions.
The Employee dataset is related to Transaction by 'id'. The objective of this pipeline is to find out the Employee who has a transaction history such that the total transactions amount from that employee doesn't  exceeds his/her salary. And such employee is considered as premium employee and their details are stored in the output location

Employee Table Example
-----------------------

id|name|company|salary
--|    | ----- | -----

1,John,ABC,15000  
2,Don ,XYZ,250000  
3,Eric,TRE,350000  
4,Sam,LKI,320000  