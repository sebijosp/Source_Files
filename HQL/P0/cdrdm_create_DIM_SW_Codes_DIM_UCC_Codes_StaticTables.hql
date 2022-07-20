--[Version History]
--0.1 - saseenthar - 6/10/2016 - intial version as part of UCC Phase 2 Requirement
-- Creates and loads data for following two static tables - cdrdm.DIM_SW_Codes , cdrdm.DIM_UCC_Codes



CREATE TABLE cdrdm.DIM_SW_Codes ( 
SW_Code Varchar(10),
Description Varchar(100),
Pick Varchar(1)) 
STORED AS ORC TBLPROPERTIES ("ORC.COMPRESS"="SNAPPY")
;

ALTER TABLE cdrdm.DIM_SW_Codes SET SERDEPROPERTIES ('serialization.encoding'='UTF-8');

INSERT INTO TABLE cdrdm.DIM_SW_Codes VALUES ('MSCHLD','Music On Hold','Y');
INSERT INTO TABLE cdrdm.DIM_SW_Codes VALUES ('RECEPT','Receptionist','Y');
INSERT INTO TABLE cdrdm.DIM_SW_Codes VALUES ('SEQRIN','Sequential Ring','Y');
INSERT INTO TABLE cdrdm.DIM_SW_Codes VALUES ('SIMRIN','Simultaneous Ring','Y');
INSERT INTO TABLE cdrdm.DIM_SW_Codes VALUES ('UCCATA','ATA Phone-Post R716','Y');
INSERT INTO TABLE cdrdm.DIM_SW_Codes VALUES ('UCCAUC','AUC','Y');
INSERT INTO TABLE cdrdm.DIM_SW_Codes VALUES ('UCCAUT','Auto-attendant','Y');
INSERT INTO TABLE cdrdm.DIM_SW_Codes VALUES ('UCCCQ','Calling Queuing-Post R716','Y');
INSERT INTO TABLE cdrdm.DIM_SW_Codes VALUES ('UCCENT','Domain (80 characters)','Y');
INSERT INTO TABLE cdrdm.DIM_SW_Codes VALUES ('UCCGSM','GSM Desk phone','Y');
INSERT INTO TABLE cdrdm.DIM_SW_Codes VALUES ('UCCHUN','HuntsGroup','Y');
INSERT INTO TABLE cdrdm.DIM_SW_Codes VALUES ('UCCIP','IP Phone (PTN for Flex and Fixed wireline)','Y');
INSERT INTO TABLE cdrdm.DIM_SW_Codes VALUES ('UCCLRN','LRN','Y');
INSERT INTO TABLE cdrdm.DIM_SW_Codes VALUES ('UCCSPT','Softphone','Y');
INSERT INTO TABLE cdrdm.DIM_SW_Codes VALUES ('UCCUSR','Linked VTN (10 digits)','Y');
INSERT INTO TABLE cdrdm.DIM_SW_Codes VALUES ('VM2EM','Email Address','Y');


CREATE TABLE cdrdm.DIM_UCC_Codes (
Code Varchar(10),
Description Varchar(100),
Category Varchar(100))
STORED AS ORC TBLPROPERTIES ("ORC.COMPRESS"="SNAPPY")
;

ALTER TABLE cdrdm.DIM_UCC_Codes SET SERDEPROPERTIES ('serialization.encoding'='UTF-8');

INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('0','Use of Service','Supplementary-Service-Action');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('100','SSC Activation','Supplementary-Service-Action');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('101','SSC Deactivation','Supplementary-Service-Action');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('102','SSC Interrogation','Supplementary-Service-Action');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('103','SSC Modification','Supplementary-Service-Action');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('104','SSC Activation request when already activate','Supplementary-Service-Action');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('105','SSC Deactivation request when already inactive','Supplementary-Service-Action');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('106','SSC Modification request with no change','Supplementary-Service-Action');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('107','SSC Invocation','Supplementary-Service-Action');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('300','Ut Service Activation','Supplementary-Service-Action');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('301','Ut Service Deactivation','Supplementary-Service-Action');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('302','Ut Interrogation','Supplementary-Service-Action');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('303','Ut Modification of Active Service Data','Supplementary-Service-Action');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('306','Ut Modification requested with no change','Supplementary-Service-Action');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('400','CCMP Conference Creation','Supplementary-Service-Action');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('401','CCMP Conference Deletion','Supplementary-Service-Action');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('402','CCMP Conference Update','Supplementary-Service-Action');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('403','CCMP User Creation','Supplementary-Service-Action');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('404','CCMP User Deletion','Supplementary-Service-Action');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('405','CCMP User Update','Supplementary-Service-Action');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('0','Forward Unconditional','Supplementary-Service-Identity - Diversion');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1','Forward on Busy','Supplementary-Service-Identity - Diversion');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1','Forward on Busy to Voicemail','Supplementary-Service-Identity - Diversion');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('2','Forward on No Reply','Supplementary-Service-Identity - Diversion');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('2','Forward on No Reply to Voicemail','Supplementary-Service-Identity - Diversion');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('3','Forward on Not Logged In','Supplementary-Service-Identity - Diversion');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('4','Forward on Not Reachable','Supplementary-Service-Identity - Diversion');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('4','Forward on Not Reachable to Voicemail','Supplementary-Service-Identity - Diversion');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('5','Forward Do-Not-Disturb','Supplementary-Service-Identity - Diversion');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('10','Forward Unconditional (complex rules)','Supplementary-Service-Identity - Diversion');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('11','Forward on Busy (complex rules)','Supplementary-Service-Identity - Diversion');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('12','Forward on No Reply (complex rules)','Supplementary-Service-Identity - Diversion');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('13','Forward on Not Logged In (complex rules)','Supplementary-Service-Identity - Diversion');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('14','Forward on Not Reachable (complex rules)','Supplementary-Service-Identity - Diversion');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('15','Forward Do-Not-Disturb (complex rules)','Supplementary-Service-Identity - Diversion');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('20','Deflection','Supplementary-Service-Identity - Diversion');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('30','Communication Diversion (CDIV)','Supplementary-Service-Identity - Diversion');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('31','Communication Diversion No Answer Timeout','Supplementary-Service-Identity - Diversion');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('100','Incoming Communication Barring (ICB)','Supplementary-Service-Identity - Barring');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('101','Outgoing Communication Barring (OCB)','Supplementary-Service-Identity - Barring');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('102','Anonymous Communication Rejection (ACR)','Supplementary-Service-Identity - Barring');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('105','Incoming Communication Allowing','Supplementary-Service-Identity - Barring');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('106','Outgoing Communication Allowing','Supplementary-Service-Identity - Barring');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('107','Do-Not-Disturb Communication Barring (DNDCB)','Supplementary-Service-Identity - Barring');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('110','ACR (complex rules)','Supplementary-Service-Identity - Barring');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('111','DNDCB (complex rules)','Supplementary-Service-Identity - Barring');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('121','Outgoing Barring Program (single program)','Supplementary-Service-Identity - Barring');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('122','Outgoing Barring Programs (multiple programs)','Supplementary-Service-Identity - Barring');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('130','Dynamic Black List (DBL)','Supplementary-Service-Identity - Barring');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('131','Malicious Communication Rejection','Supplementary-Service-Identity - Barring');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('140','National Toll Restriction','Supplementary-Service-Identity - Barring');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('141','International Toll Restriction','');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('200','Conference Creation','Supplementary-Service-Identity - Conference ');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('201','Participant Addition','Supplementary-Service-Identity - Conference ');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('202','Participant Removal','Supplementary-Service-Identity - Conference ');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('203','Conference Completion','Supplementary-Service-Identity - Conference ');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('204','Scheduled Conference (administration)','Supplementary-Service-Identity - Conference ');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('205','Participant Join ','Supplementary-Service-Identity - Conference ');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('206','Participant Leave ','Supplementary-Service-Identity - Conference ');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('207','Conference Active ','Supplementary-Service-Identity - Conference ');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('208','Conference Inactive ','Supplementary-Service-Identity - Conference ');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('209','Participant Update','Supplementary-Service-Identity - Conference ');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('210','Transferred to Conference','Supplementary-Service-Identity - Conference ');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('220','Event Package Subscription','Supplementary-Service-Identity - Conference ');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('400','Communication Waiting','Communication Waiting');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('500','Flexible Communication Distribution (FCD)','Flexible Communication Distribution');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('501','Serial FCD','Flexible Communication Distribution');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('502','Parallel FCD','Flexible Communication Distribution');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('503','FCD Divert Primary','Flexible Communication Distribution');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('504','Flexible FCD','Flexible Communication Distribution');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('600','Originating Identity Presentation (OIP)','Identity Presentation');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('601','Originating Identity Restriction (OIR)','Identity Presentation');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('602','Terminating Identity Presentation (TIP)','Identity Presentation');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('603','Terminating Identity Restriction (TIR)','Identity Presentation');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('606','Calling Name Identity Presentation','Identity Presentation');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('607','Flexible Identity Presentation','Identity Presentation');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('608','Originating Calling Name Identity Presentation','Identity Presentation');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('700','Supplementary Service Codes','Supplementary Service Codes');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('701','Generic Supplementary Service Codes','Supplementary Service Codes');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('800','Permanent MCID','Malicious Communication Identification (MCID)');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('801','Temporary MCID','Malicious Communication Identification (MCID)');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1000','Abbreviated Dialing','Abbreviated Dialing');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1100','User Call Admission Control','Call Admission Control');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1101','Group Call Admission Control','Call Admission Control');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1200','Three Party Session','Three Party (3PTY)');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1201','Two Party Session - Transition to Three Party','Three Party (3PTY)');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1202','Two Party Session - Transition to Two Party','Three Party (3PTY)');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1203','Two Party Session - In Three Party','Three Party (3PTY)');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1300','Carrier Select','Carrier Selection');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1301','Carrier Pre-Select','Carrier Selection');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1302','Carrier Pre-Select local','Carrier Selection');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1303','Carrier Pre-Select non-local','Carrier Selection');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1400','CC Busy Subscriber (CCBS) Activation','Communication Completion (CC)');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1401','CC Busy Subscriber (CCBS) Request','Communication Completion (CC)');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1402','CC Busy Subscriber (CCBS) Call','Communication Completion (CC)');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1403','CC Busy Subscriber (CCBS) Revocation','Communication Completion (CC)');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1410','CC No Reply (CCNR) Activation','Communication Completion (CC)');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1411','CC No Reply (CCNR) Request','Communication Completion (CC)');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1412','CC No Reply (CCNR) Call','Communication Completion (CC)');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1413','CC No Reply (CCNR) Revocation','Communication Completion (CC)');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1421','CC Not Logged-in (CCNL) Request','Communication Completion (CC)');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1422','CC Not Logged-in (CCNL) Call','Communication Completion (CC)');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1423','CC Not Logged-in (CCNL) Revocation','Communication Completion (CC)');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1500','Short Number Dialing','Short Number Dialing');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1600','Requested Announcement','Requested Announcement');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1700','User Common Data','User Common Data');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1800','Long-Distance Mobile Number Policing','Address Policing');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('1900','Explicit Communication Transfer','Address Policing');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('2000','Calling Party Category','Calling Party Category');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('2100','Call Notification / Call Notification in MMTel','Parlay X');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('2101','3PCC Session','Parlay X');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('2200','Number Portability Outgoing','Number Portability');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('2201','Number Portability Incoming','Number Portability');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('2300','Session Transfer to Own Device','Session Transfer to Own Device');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('2301','Serial STOD','Session Transfer to Own Device');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('2302','Parallel STOD','Session Transfer to Own Device');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('2303','Flexible STOD','Session Transfer to Own Device');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('2400','Customized Alerting Tones','Customized Alerting Tones');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('2500','Flexible Service Format Selection','Flexible Service Format Selection');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('2600','Call Return without announcement prompt','Call Return');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('2601','Call Return with announcement prompt','Call Return');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('2700','Unconditional hotline','Hotline');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('2701','Instant hotline','Hotline');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('2702','Delayed hotline','Hotline');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('2703','Delayed hotline voicemail','Hotline');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('2800','Dialed Number Mapping','Dialed Number Mapping');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('3200','Closed User Group','Closed User Group');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('3300','Multi Subscriber Number','Multi Subscriber Number');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('3400','Distinctive Ring with mapping','Distinctive Ring');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('3401','Distinctive Ring without mapping','Distinctive Ring');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('3500','Operator Controlled Transfer','Operator Controlled Transfer');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('9001','Mobility','UCC Specific');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('9002','Shared Called Apprearence (SCA)','UCC Specific');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('9003','Hunt-Group','UCC Specific');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('9004','Deflection','UCC Specific');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('9005','Sequential Ringing','UCC Specific');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('9006','Simultaneous Ringing','UCC Specific');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('9007','Transfer','UCC Specific');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('9008','Recall','UCC Specific');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('9009','Call Pull','UCC Specific');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('9010','ClickToDial','UCC Specific');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('9011','Parked','UCC Specific');
INSERT INTO TABLE cdrdm.DIM_UCC_Codes VALUES ('9013','Call Centre','UCC Specific');