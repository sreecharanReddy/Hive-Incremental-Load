/********************************************inputs********************************************/
DECLARE
@TotDays int =1,						----For How many days appointments need to be created
@Da int = 25,							----How many appointments per day
@did bigint = 11,						----For which Doctor you need to create appointments
							
@date1 datetime = CAST(CAST(getdate()-7 AS DATE) AS DATETIME)					----From which Date you need to create appointments
/********************************************inputs********************************************/



/*Script will perform following tasks for single patient:
1.create appointment
2.bill Appointments
3.Create service orders
4.insert chief summary, complaints, vitals & medications
5.bill service orders


*/
declare @COUNT INT, @LABID INT = 1
,@LabStageName Varchar(100) 
,@RadStageName Varchar(100) 



/********************************************inputs********************************************/
DECLARE
@NoOfDays int =@TotDays,								----For How many days appointments need to be created
@DailyAppointments1 int = @da ,					----How many appointments per day
@doctorid bigint = @did,						----For which Doctor you need to create appointments
							----For which patient you need to create appointments
@date datetime = @date1,						----From which Date you need to create appointments
/********************************************inputs********************************************/

@ConsultationTypeId int, 
@DCount int, @Weekday Varchar(20), @time time, @id int, @OB INT, @AppointmentId int,
@DailyAppointments int, @Upper INT = 5, @Lower INT = -5, @ChiefComplaintId  int,@OrderId int
,@patientid int ,@Interval int




DECLARE 
--@STARTDATE DATE = '2017-04-21 00:00:00.000'			--from date
--, @ENDDATE DATE = '2017-04-22 00:00:00.000'					--to date
--, @patientId int = 374212
---------------------------------------parameters-------------------------------------

 @PatientClassCode Varchar(10) = 'PAT'
,@PatientClassId BIGINT
,@ArrivalDate DATETIME
,@InvestingationAttendDate DATETIME
,@InvestingationResultDate DATETIME
,@InvestingationExitDate DATETIME
,@ResultAuthenticationDate DATETIME
,@DispatchDate DATETIME
,@ReportAcknowledgementDate DATETIME
,@ReportIssueDate DATETIME



IF OBJECT_ID('tempdb..#Order') IS NOT NULL
DROP TABLE #Order

CREATE TABLE #Order
(OrderId bigint, OrderDate DATETIME, PatientId BIGINT)

IF OBJECT_ID('tempdb..#patient') IS NOT NULL
DROP TABLE #patient

SELECT TOP 1 PatientId, 0 AS IsUsed , DOB INTO #patient
FROM [dbo].[Patient]
--WHERE PatientId =  @patientid
ORDER BY NEWID()




WHILE (@NoOfDays> 0)
BEGIN

SET @DailyAppointments = @DailyAppointments1


SET @Weekday = DATENAME(WEEKDAY, @date)

UPDATE #patient
SET IsUsed  = 0

WHILE (@DailyAppointments > 0)
BEGIN

	SELECT TOP 1 @patientid = PatientId
	FROM #patient
	WHERE IsUsed  = 0
	ORDER BY NEWID()
	



	INSERT INTO [dbo].[Order]
		([OrderNumber]
		,[PatientId]
		,[DoctorId]
		,[AppointmentId]
		,[OrderDate]
		,[CreatedById]
		,[UpdatedById]
		,[CreatedTime]
		,[LastUpdatedTime]
		,[IsActive]
		,[DeleteComments]
		,[HospitalId]
		,[AdmissionNumber]
		,[AdmittingConsultantId]
		,[ServiceRequestRemark]
		,[ServiceAppointmentId]
		,[IsFromBilling]
		,[PrimaryDoctorId]
		,[IsCancelled])
	SELECT NEXT VALUE FOR ORDERNUMBER
		,@patientid
		,@doctorid
		,null
		,@date
		,1,1,@DATE, @date,1,NULL, 1, NULL, NULL, NULL, NULL, NULL, @doctorid, 0


	select @OrderId = @@IDENTITY

	INSERT INTO #Order
	(OrderId, OrderDate, PatientId)
	SELECT OrderId, OrderDate, PatientId
	FROM [Order]
	WHERE OrderId = @OrderId

					  
	INSERT INTO [dbo].[OrderServiceMap]
		([OrderId]
		,[ServiceId]
		,[CreatedById]
		,[UpdatedById]
		,[CreatedTime]
		,[LastUpdatedTime]
		,[IsActive]
		,[DeleteComments]
		,[IsBilled]
		,[DepartmentId]
		,[SectionId]
		,[HospitalId]
		,[Stat]
		,[Quantity]
		,[Remarks])
	SELECT @OrderId
		,ServiceId
		,1,1,@DATE, @date, 1, NULL, 1,SEC.DepartmentId ,SEC.SectionId, 1, 'N', 1, NULL
	FROM 
	(select top 5 ServiceId ,SectionId 
		from 
		(
		select top 1 ServiceId,SectionId 
		from mst.Service  
		WHERE ServiceName IN ('X Ray Chest PA','CT Scan Abdomen','X Ray Abdomen')
		order by newid()							
		union 
		select top 1 ServiceId ,SectionId 
		from mst.Service  
		WHERE ServiceName IN ('Ammonia','Serum Sodium', 'Lipid Profile') 
		order by newid() 
		)t10
		order by newid()
		)S 
	INNER JOIN mst.Section SEC
	ON SEC.SectionId = s.SectionId

	UPDATE #patient
	SET IsUsed  = 1
	WHERE PatientId = @patientid

	SET @DailyAppointments = @DailyAppointments-1;
END
		




----------
SELECT @PatientClassId  = PatientClassId
FROM mst.PatientClass
WHERE PatientClassCode = @PatientClassCode

-----------------INVOICE HEADER 
INSERT INTO Bill_InvoiceHeader
		(InvoiceNumber
		,PatientId
		,InvoiceAmount
		,InvoiceDate
		,SettledAmount
		,BalanceAmount
		,PatientGrossAmount
		,CreatedById
		,UpdatedById
		,CreatedTime
		,LastUpdatedTime
		,IsActive
		,HospitalId
		,[OrderId]
		,PatientTypeId
		,PatientClassCode
		)
		
SELECT  Prefix + '-' +  CAST(NEXT VALUE FOR [dbo].[Seq_INV_Number] AS VARCHAR) + '-' + Suffix ,
		O.PatientId,
		0,
		O.OrderDate,
		0,
		0,
		0,
		1,
		1,
		GETDATE(),
		GETDATE(),
		1,
		1,
		O.OrderId,
		@PatientClassId,
		@PatientClassCode 
FROM mst.SequenceGenerator SG
INNER JOIN #Order O
ON 1 = 1
WHERE Prefix='OAD'
--PaymentType = 'Invoice'

------------------INVOICE DETAILS
INSERT INTO Bill_InvoiceDetail
		(InvoiceId
		,PatientId
		,ServiceId
		,ServiceAmount
		,Unit
		,PatientGrossAmount
		,PatientNetAmount
		,SponsorGrossAmount
		,SponsorNetAmount
		,InvoiceIndicator
		,CreatedById
		,UpdatedById
		,CreatedTime
		,LastUpdatedTime
		,IsActive
		,HospitalId
		,OrderId)
		
SELECT	BIH.InvoiceId,
		O.PatientId,
		OSM.ServiceId, 
		T.ServiceTariff, 
		1, 
		CASE WHEN PS.PatientSponsorId IS NOT NULL THEN 0 ELSE T.ServiceTariff END, 
		CASE WHEN PS.PatientSponsorId IS NOT NULL THEN 0 ELSE T.ServiceTariff END, 
		CASE WHEN PS.PatientSponsorId IS NULL THEN 0 ELSE ISNULL(STD.InstituteTariff,T.ServiceTariff) END, 
		CASE WHEN PS.PatientSponsorId IS NULL THEN 0 ELSE ISNULL(STD.InstituteTariff,T.ServiceTariff) END, 
		'INV', 
		1, 
		1, 
		GETDATE(),
		GETDATE(),
		1, 
		1,
		OSM.OrderId
FROM OrderServiceMap OSM
INNER JOIN Bill_InvoiceHeader BIH
ON BIH.OrderId = OSM.OrderId
INNER JOIN Tariff T
ON OSM.ServiceId = T.ServiceId
INNER JOIN #Order O
ON O.OrderId = OSM.OrderId
LEFT JOIN PatientSponsor PS
ON PS.PatientId = BIH.PatientId
AND PS.IsActive = 1
LEFT JOIN Entitlement E
ON E.EntitlementId = PS.EntitlementId
INNER JOIN mst.PatientClass PC
ON PC.PatientClassCode = 'PAT'
AND T.ServiceTariffPatClassId = PC.PatientClassId
LEFT JOIN ServiceTariffDefinition STD
ON STD.ServiceId = OSM.ServiceId
AND STD.PatientClassId = PC.PatientClassId
AND STD.IsActive = 1 
AND STD.InstituteId = E.InstituteId



UPDATE BIH
SET BIH.InvoiceAmount =  BID.ServiceAmount ,
BIH.SettledAmount = BID.ServiceAmount ,
BIH.PatientGrossAmount =  BID.PatientGrossAmount,
BIH.PatientNetAmount =  BID.PatientNetAmount,
BIH.SponsorGrossAmount = BID.SponsorGrossAmount,
BIH.SponsorNetAmount= BID.SponsorNetAmount
FROM Bill_InvoiceHeader BIH
JOIN (	SELECT InvoiceId, SUM(ServiceAmount) AS ServiceAmount, SUM(PatientNetAmount) AS PatientNetAmount, SUM(PatientGrossAmount) AS PatientGrossAmount, 
		SUM(SponsorGrossAmount) AS SponsorGrossAmount, SUM(SponsorNetAmount) AS SponsorNetAmount
		FROM Bill_InvoiceDetail 
		GROUP BY InvoiceId) AS BID
ON BIH.InvoiceId = BID.InvoiceId
INNER JOIN #Order O
ON O.OrderId = BIH.OrderId 



INSERT INTO Bill_Receipt
        (ReceiptNumber
        ,PatientId
        ,ReceiptAmount
        ,ReceiptDate
        ,ReceiptIndicator
        ,CreatedById
        ,UpdatedById
        ,CreatedTime
        ,LastUpdatedTime
        ,IsActive
        ,HospitalId
        ,OrderId
        ,InvoiceId)

SELECT Prefix + '-' +  CAST(NEXT VALUE FOR [dbo].[Seq_OPReceipt_Number] AS VARCHAR) + '-' + Suffix ,
		O.PatientId,
		InvoiceAmount,
		InvoiceDate,
		'INV',
		1,
		1,
		GETDATE(),
		GETDATE(),
		1,
		1,
		O.OrderId,
		InvoiceId
FROM Bill_InvoiceHeader BIH
INNER JOIN mst.SequenceGenerator  SG
ON BIH.IsActive = 1 AND SG.IsActive = 1
INNER JOIN #Order O
ON O.OrderId = BIH.OrderId 
LEFT JOIN PatientSponsor PS 
ON PS.PatientId = O.PatientId 
AND PS.IsActive = 1
WHERE 
Prefix='OAD' and
--PaymentType = 'OP Receipt' 
--AND 
PS.PatientSponsorId IS NULL
	
	
INSERT INTO Bill_ReceiptInvoiceMap
		(ReceiptId
		,ReceiptAmount
		,UtilizedAmount
		,CreatedById
		,UpdatedById
		,CreatedTime
		,LastUpdatedTime
		,IsActive
		,HospitalId
		,InvoiceId)

SELECT  ReceiptId,
		ReceiptAmount,
		0,
		1,
		1,
		GETDATE(),
		GETDATE(),
		1,
		1,
		BIH.InvoiceId
FROM Bill_Receipt R
INNER JOIN #Order O
ON O.OrderId = R.OrderId
INNER JOIN Bill_InvoiceHeader BIH
ON BIH.OrderId= O.OrderId

	
INSERT INTO Bill_PaymentByCash
		(ReceiptId
		,Amount
		,CreatedById
		,UpdatedById
		,CreatedTime
		,LastUpdatedTime
		,IsActive
		,HospitalId
		,CurrencyId
		,BaseAmount)
SELECT  R.ReceiptId,
		R.ReceiptAmount,
		1,
		1,
		GETDATE(),
		GETDATE(),
		1,
		1,
		2,
		ReceiptAmount
FROM Bill_Receipt R
INNER JOIN [#Order] O
ON O.OrderId = R.OrderId
INNER JOIN Bill_InvoiceHeader BIH
ON BIH.OrderId= O.OrderId



	--SELECT @CollectionDate = DATEADD(HH, 8+CAST((RAND()*99) AS INT)%2, @StartDate)
	--SELECT @DistributionDate = DATEADD(HH, CAST((RAND()*99) AS INT)%2 + 24*CAST((RAND()*99) AS INT)%3, @CollectionDate)
	--SELECT @ProcessDate = DATEADD(HH, CAST((RAND()*99) AS INT)%2 + 24*CAST((RAND()*99) AS INT)%4, @DistributionDate)
	--SELECT @ResultEntryDate = DATEADD(HH, CAST((RAND()*99) AS INT)%2 + 24*CAST((RAND()*99) AS INT)%2, @DistributionDate)
	--SELECT @ResultAuthenticateDate = DATEADD(HH, CAST((RAND()*99) AS INT)%2 + 24*CAST((RAND()*99) AS INT)%3, @DistributionDate)
	--SELECT @ReportDispatchedDate = DATEADD(HH, CAST((RAND()*99) AS INT)%2 + 24*CAST((RAND()*99) AS INT)%2, @ResultAuthenticateDate)
	--SELECT @ReportAcknowledgementDate = DATEADD(HH, CAST((RAND()*99) AS INT)%2 + 24*CAST((RAND()*99) AS INT)%2, @ReportDispatchedDate)
	--SELECT @ReportIssueDate = DATEADD(HH, CAST((RAND()*99) AS INT)%2,  @ReportAcknowledgementDate)



INSERT INTO [dbo].[LabTransaction]
(	 [OrderId]
    ,[ServiceId]
    ,[PatientId]
    ,[RegistrationNumber]
    ,[SectionId]
    ,[CurrentStageId]
    ,[AccessionNumber]
    ,[AuthenticatingDoctorId]
    ,[PrescribingDoctorId]
    ,[CollectionDate]
    ,[CreatedById]
    ,[UpdatedById]
    ,[CreatedTime]
    ,[LastUpdatedTime]
    ,[IsActive]
    ,[HospitalId]
    ,[DistributionDate]
    ,[ProcessDate]
    ,[Stat]
    ,[TransportedBy]
    ,[LabReasonId]
    ,[ExternalAgencyId]
    ,[OrderDate]
    ,[PatientAge]
    ,[AgeUnitId]
    ,[CollectedById]
    ,[DistributedById]
    ,[ProcessedById]
    ,[ResultEntryById]
    ,[ResultAuthenticatedById]
    ,[ReportDispatchedById]
    ,[ReportAcknowledgedById]
    ,[ReportIssuedById]
    ,[LabContainerId]
    ,[LabSampleTypeId]
    ,[ResultEntryDate]
    ,[ResultAuthenticateDate]
    ,[ReportDispatchedDate]
    ,[ReportAcknowledgementDate]
    ,[FailedAttemptById]
    ,[FailedAttemptDate]
    ,[RecollectionById]
    ,[RecollectionDate]
    ,[ReportIssueDate]
    ,[ReAuthenticateReasonId]
    ,[LabResultType])
SELECT 
	O.OrderId
    ,ISNULL(PNL.ComponentId,OSM.ServiceId)
    ,P.PatientId
    ,P.RegistrationNumber
    ,S.sectionId
    ,LS.LabStageId AS [CurrentStageId]
    ,CAST((RAND()*99) AS INT)%30 AS [AccessionNumber]
    ,NULL AS [AuthenticatingDoctorId]
    ,NULL AS [PrescribingDoctorId]
    ,DATEADD(MINUTE, 120+CAST((RAND()*9900) AS INT)%60, OrderDate) AS [CollectionDate]
    ,1
    ,1
    ,O.OrderDate
    ,O.OrderDate
    ,1 AS [IsActive]
    ,1 AS [HospitalId]
    ,DATEADD(HH, 3+CAST((RAND()*99) AS INT)%2 , OrderDate) AS [DistributionDate]
    ,DATEADD(HH, 4+CAST((RAND()*99) AS INT)%2 , OrderDate) AS [ProcessDate]
    ,CASE WHEN CAST((RAND()*99) AS INT)%2 = 0 THEN 'Y' ELSE 'N' END AS [STAT]
    ,LT.LabTechnicianId AS [TransportedBy]
    ,NULL AS [LabReasonId]
    ,EA.ExternalAgencyId AS [ExternalAgencyId]
    ,OrderDate
	,CASE WHEN DATEDIFF(MONTH, P.DOB, GETDATE()) < 1 
		THEN DATEDIFF(DD, P.DOB, GETDATE()) 
		WHEN DATEDIFF(YEAR, P.DOB, GETDATE()) < 1 
		THEN DATEDIFF(MONTH, P.DOB, GETDATE())
		ELSE DATEDIFF(YEAR, P.DOB, GETDATE()) 
	END AS [PatientAge]
    ,CASE WHEN DATEDIFF(MONTH, P.DOB, GETDATE()) < 1 AND AU.AgeUnitName = 'DAYS'
		THEN AU.AgeUnitId
		WHEN DATEDIFF(YEAR, P.DOB, GETDATE()) < 1 AND AU.AgeUnitName = 'MONTHS'
		THEN AU.AgeUnitId
		WHEN DATEDIFF(YEAR, P.DOB, GETDATE()) > 1 AND AU.AgeUnitName = 'YEARS'
		THEN AU.AgeUnitId
	END AS [AgeUnitId]
    ,LT.LabTechnicianId AS [CollectedById]
    ,LT.LabTechnicianId AS [DistributedById]
    ,LT.LabTechnicianId AS [ProcessedById]
    ,LT.LabTechnicianId AS [ResultEntryById]
    ,LT.LabTechnicianId AS [ResultAuthenticatedById]
    ,LT.LabTechnicianId AS [ReportDispatchedById]
    ,LT.LabTechnicianId AS [ReportAcknowledgedById]
    ,LT.LabTechnicianId AS [ReportIssuedById]
    ,LC.[LabContainerId] AS [LabContainerId]
    ,LST.[LabSampleTypeId] AS [LabSampleTypeId]
    ,DATEADD(HH, 5+CAST((RAND()*99) AS INT)%2 , OrderDate) AS [ResultEntryDate]
    ,DATEADD(HH, 6+CAST((RAND()*99) AS INT)%2 , OrderDate) AS [ResultAuthenticateDate]
    ,DATEADD(HH, 7+CAST((RAND()*99) AS INT)%2 , OrderDate) AS [ReportDispatchedDate]
    ,DATEADD(HH, 8+CAST((RAND()*99) AS INT)%2 , OrderDate) AS [ReportAcknowledgementDate]
    ,NULL AS [FailedAttemptById]
    ,NULL AS [FailedAttemptDate]
    ,NULL AS [RecollectionById]
    ,NULL AS [RecollectionDate]
    ,DATEADD(HH, 9+CAST((RAND()*99) AS INT)%2 , OrderDate) AS [ReportIssueDate]
    ,NULL AS [ReAuthenticateReasonId]
    ,LRT.LabResultTypeName AS [LabResultType]
FROM #Order O
INNER JOIN Patient P
ON P.PatientId = O.PatientId
INNER JOIN mst.LabStage LS 
ON LS.LabStageName = 'ReportIssued'
AND LS.IsActive = 1
LEFT JOIN (SELECT TOP 1 * FROM  mst.LabTechnician WHERE IsActive = 1 ORDER BY NEWID()) LT
ON LT.IsActive = 1
LEFT JOIN (SELECT TOP 1 * FROM  [mst].[ExternalAgency]  WHERE IsActive = 1 ORDER BY NEWID())EA
ON EA.IsActive = 1
INNER JOIN (SELECT TOP 1 * FROM  mst.AgeUnit  WHERE IsActive = 1 ORDER BY NEWID())AU
ON AU.IsActive = 1
INNER JOIN OrderServiceMap OSM
ON O.OrderId = OSM.OrderId
LEFT JOIN mst.Panel PNL
ON PNL.PanelId = OSM.ServiceId
AND PNL.IsActive = 1
INNER JOIN mst.LabInvestigation LI
ON LI.ServiceId = ISNULL(PNL.ComponentId, OSM.ServiceId)
left JOIN mst.LabResultType LRT
ON LRT.LabResultTypeId = LI.LabResultTypeId
INNER JOIN mst.Service S 
ON S.SERVICEID = OSM.ServiceId
INNER JOIN mst.Section SC 
ON SC.SectionId = S.SectionId
LEFT JOIN (SELECT TOP 1 * FROM [mst].[LabSampleType] WHERE IsActive = 1 ORDER BY NEWID())LST
ON LST.IsActive = 1
LEFT JOIN (SELECT TOP 1 * FROM [mst].[LabContainer]  WHERE IsActive = 1 ORDER BY NEWID())LC
ON LC.IsActive = 1




IF OBJECT_ID('TEMPDB..#RESULT' ) IS NOT NULL
DROP TABLE #RESULT
CREATE TABLE #Result
( service VARCHAR(255),MinimumNormalRange INT,MaximumNormalRange INT,MinimumCriticalRange INT,MaximumCriticalRange INT)
INSERT INTO #Result

VALUES 
 ('Ammonia','9','50','4','60')
,('Serum Sodium','135','145','125','160')
,('HDL Cholesterol','40','50','30','60')
,('LDL Cholesterol','90','100','80','120')
,('Total Cholesterol','150','200','100','230')
,('Triglycerides','100','150','80','200')

INSERT INTO [dbo].[LabSpecimenResult]
	([LabTransactionId]
	,[LabParameterId]
	,[HospitalId]
	,[Result]
	,[CreatedById]
	,[Comment]
	,[UpdatedById]
	,[CreatedTime]
	,[LastUpdatedTime]
	,[IsActive]
	,[MinimumValue]
	,[MaximumValue]
	,[CriticalMinimumValue]
	,[CriticalMaximumValue]
	,[TextRange]
	,[Method])
SELECT LT.[LabTransactionId]
	,NULL AS [LabParameterId]
	,1 AS [HospitalId]
	,CASE WHEN [LabTransactionId] %4 = 0 THEN MinimumNormalRange+CAST(RAND()*LabTransactionId*9098 AS INT)%(MinimumNormalRange-MaximumNormalRange )
	ELSE MinimumCriticalRange+CAST(RAND()*LabTransactionId*9099 AS INT)%(MinimumCriticalRange-MaximumCriticalRange )
	END AS [Result]
	,1 AS [CreatedById]
	,NULL AS [Comment]
	,1 AS [UpdatedById]
	,LT.[ResultEntryDate] AS [CreatedTime]
	,LT.[ResultEntryDate] AS [LastUpdatedTime]
	,1 AS [IsActive]
	,MinimumNormalRange
	,MaximumNormalRange
	,MinimumCriticalRange
	,MaximumCriticalRange
	,NULL AS [TextRange]
	,NULL AS [Method]
FROM LabTransaction LT
INNER JOIN [#Order] O
ON O.OrderId = LT.OrderId
INNER JOIN mst.Service S 
ON S.ServiceId = LT.ServiceId
INNER JOIN #Result R 
ON R.service = S.ServiceName




INSERT INTO [dbo].[Rad_Transaction]
(	 [OrderId]
    ,[ServiceId]
    ,[PatientId]
    ,[RegistrationNumber]
    ,[DepartmentId]
    ,[SectionId]
    ,[ArrivalDate]
    ,[InvestingationAttendDate]
    ,[InvestingationResultDate]
    ,[InvestingationExitDate]
    ,[ResultAuthenticationDate]
    ,[DispatchDate]
    ,[ReportAcknowledgementDate]
    ,[ReportIssueDate]
    ,[InvestigationResultImage]
    ,[ReasonForDelay]
    ,[TechnicianId]
    ,[Remarks]
    ,[AuthenticatingDoctorId]
    ,[CreatedById]
    ,[UpdatedById]
    ,[CreatedTime]
    ,[LastUpdatedTime]
    ,[IsActive]
    ,[DeleteComments]
    ,[HospitalId]
    ,[CurrentStageId]
    ,[PreviousStageId]
    ,[TestCompletionTime]
    ,[Comments]
    ,[Template]
    ,[RadiologyTemplateId]
    ,[AccessionNumber]
    ,[Opinion]
    ,[ClinicalDiagnosis]
    ,[OrderServiceMapId]
    ,[ResultEntryById]
    ,[ReportDispatchedById]
    ,[ReportIssuedById]
    ,[ArrivalEntryById]
    ,[InvestingationAttendedById]
    ,[PatientAge]
    ,[ReportAcknowledgementById]
    ,[PrintOpinion]
    ,[PrintClinicalDiagnosis])
SELECT 
	O.OrderId
    ,OSM.ServiceId
    ,P.PatientId
    ,P.RegistrationNumber
    ,RE.DepartmentId
    ,S.SectionId
    ,DATEADD(HH, 3, O.OrderDate )
    ,DATEADD(HH, 4, O.OrderDate ) 
    ,DATEADD(HH, 5, O.OrderDate ) 
    ,DATEADD(HH, 6, O.OrderDate ) 
    ,DATEADD(HH, 7, O.OrderDate ) 
    ,DATEADD(HH, 8, O.OrderDate ) 
    ,DATEADD(HH, 9, O.OrderDate ) 
    ,DATEADD(HH, 10, O.OrderDate ) 
    ,NULL AS IMAGE
    ,NULL
    ,RT.[RadiologyTechnicianId]
    ,Remarks
    ,D.DoctorId AS AuthenticatingDoctorId
    ,1
    ,1
    ,GETDATE()
    ,GETDATE()
    ,1
    ,NULL
    ,1
    ,RS.Rad_StageId AS CurrentStageId
    ,RS.Rad_StageId - 1 AS PreviousStageId
    ,NULL
    ,NULL
    ,(SELECT TOP 1 Template FROM [mst].[RadiologyTemplate] RT WHERE IsActive = 1 AND RT.ServiceId = OSM.ServiceId  ORDER BY S.ServiceId) AS Template 
    ,(SELECT TOP 1 RadiologyTemplateId FROM [mst].[RadiologyTemplate] RT WHERE IsActive = 1 AND RT.ServiceId = OSM.ServiceId  ORDER BY S.ServiceId) AS RadiologyTemplateId
    ,NULL AS AccessionNumber
    ,NULL AS Opinion
    ,NULL AS ClinicalDiagnosis
    ,OrderServiceMapId
    ,RT.[RadiologyTechnicianId] AS  ResultEntryById
    ,RT.[RadiologyTechnicianId] AS ReportDispatchedById
    ,RT.[RadiologyTechnicianId] AS ReportIssuedById
    ,RT.[RadiologyTechnicianId] AS ArrivalEntryById
    ,RT.[RadiologyTechnicianId] AS InvestingationAttendedById
    ,CASE WHEN DATEDIFF(MONTH, P.DOB, GETDATE()) < 1 
		THEN DATEDIFF(DD, P.DOB, GETDATE()) 
		WHEN DATEDIFF(YEAR, P.DOB, GETDATE()) < 1 
		THEN DATEDIFF(MONTH, P.DOB, GETDATE())
		ELSE DATEDIFF(YEAR, P.DOB, GETDATE()) 
	END AS [PatientAge]
    ,NULL AS ReportAcknowledgementById
    ,0 AS PrintOpinion
    ,0 AS PrintClinicalDiagnosis
FROM #Order O
INNER JOIN Patient P
ON P.PatientId = O.PatientId
INNER JOIN OrderServiceMap OSM
ON OSM.OrderId =O.OrderId
INNER JOIN [mst].[RadiologyExamination] RE
ON OSM.ServiceId = RE.ServiceId
AND RE.IsActive = 1
LEFT JOIN (SELECT TOP 1 [RadiologyTechnicianId], IsActive FROM [mst].[RadiologyTechnician] WHERE IsActive = 1 ORDER BY NEWID()) RT
ON RT.IsActive = 1
LEFT JOIN (	SELECT TOP 1 D.DoctorId
			FROM mst.Doctor D 
			INNER JOIN mst.RadiologyExamination RE
			ON RE.DepartmentId = D.DepartmentId
			AND RE.IsActive = 1
			AND D.IsActive = 1 
			ORDER BY NEWID()
			) D
ON 1 = 1
LEFT JOIN [mst].[Rad_Stage] RS 
ON RS.StageName  = 'Report Issue'
INNER JOIN mst.Service S 
ON S.ServiceId = OSM.ServiceId

	  
	  
	  
INSERT INTO [dbo].[Rad_TransactionAudit]
([TransactionId]
    ,[CreatedById]
    ,[UpdatedById]
    ,[CurrentStageId]
    ,[CreatedTime]
    ,[LastUpdatedTime]
    ,[IsActive]
    ,[AuthenticationReason])
SELECT 
	[TransactionId]
	,1
	,1
	,RS.Rad_StageId
	,GETDATE()
	,GETDATE()
	,1
	,NULL
FROM #Order O
INNER JOIN Rad_Transaction RD 
ON RD.OrderId = O.OrderId
INNER JOIN mst.Rad_Stage RS 
ON RS.Rad_StageId <= (SELECT Rad_StageId FROM mst.Rad_Stage WHERE StageName = 'Authentication')
	  





SET @NoOfDays = @NoOfDays-1;

SET @date = dateadd(dd, 1, @Date);
END
	 