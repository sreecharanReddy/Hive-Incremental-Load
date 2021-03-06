
UPDATE mst.Bed
SET BedStatusCode = 'V'
WHERE BedCurrentAdmissionNumber IS NULL
AND BedStatusCode != 'V'

set xact_abort on;

DECLARE 
/********************************Parameters**********************************************/
@StartDate DATETIME = '2017-12-04 00:00:00.000',	-- Start Date for the loop to create reservation
@EndDate DATETIME = '2017-12-07 00:00:00.000',		-- End Date for the loop to create reservation
@expadmdays int = 1 ,								-- Difference between exepcted admission date & Reservation date
@PaymentType VARCHAR(20) = 'SELF PAYING',			-- 'SELF PAYING'/'SPONSOR'
@RsDailyCount INT = 1,								-- per day per Reservation count.
@DoctorId bigint =  11,								-- Doctor Id of the Amditting consultant
@PatientId BIGINT ,									
@TreatmentType VARCHAR(50) =  'Medical' ,			-- Treatment type selection
@ExpectedLOS int = 03,								-- expected los days
@WardName VARCHAR (255) = 'GENERAL MEDICAL WARD',	-- Select Ward name for bed allocation
@ChiefComplaint VARCHAR (255)  = 'Abdominal Pain',	-- Select Chief Complaint 
@IntensiveCareUnit VARCHAR(255) = 'INTENSIVE CARE UNIT',
@DrugApprovedQuantity BIGINT = 10,
@DrugIssuedQuantity BIGINT = 10,
@DischargePatient int = 0, --- Set 1 if you want to Dicharge these patients else set 0.
@NeedWardTransfer int = 0,  ---Set 1 if you want to do location transfer for these patients else set 0
/*--make sure you ward mentioned above are present in mst.ward.
SELECT W.WardName, COUNT(1)
FROM mst.Bed B 
INNER JOIN mst.Ward W
ON W.WardId = B.WardId
WHERE BedCurrentAdmissionNumber IS NULL
AND WARDNAME IN ('GENERAL MEDICAL WARD', 'INTENSIVE CARE UNIT')
GROUP BY WardName

SELECT * FROM  mst.Ward W
SELECT * FROM  mst.doctor
where doctorid = 20828


*/
/***************************************************************************************/


@RsCount INT ,
@UserId BIGINT = 1,
@ReservationNumber BIGINT,
@AdmissionId BIGINT,
@IPServiceRequestHeaderId bigint,
@IPPackageTransferId BIGINT,
@AdmissionDate Datetime,
@IPBillInvoiceHeaderId BIGINT,
@DrugCount  INT,
@DischargeSummaryHeaderId BIGINT,
@DischargeDateTime Datetime
SET @PaymentType = 'SELF PAYING'
IF OBJECT_ID('TEMPDB..#Service') IS NOT NULL
BEGIN 
DROP TABLE #Service
END


CREATE TABLE #Service 
(ServiceId bigint, 
ServiceDefaultRender VARCHAR(1))


INSERT INTO #Service
SELECT ServiceId, ServiceDefaultRender
FROM mst.Service
WHERE SERVICENAME IN 
(
 'Ammonia'
,'Serum Sodium'
,'HDL Cholesterol'
,'LDL Cholesterol'
,'Total Cholesterol'
,'Triglycerides'
,'DC ABDOMEN'
,'DC CT SCAN OF ABDOMEN'
,'DC AVENUE ABDOMEN'
)

IF OBJECT_ID('TEMPDB..#Product') IS NOT NULL
BEGIN 
DROP TABLE #Product
END


CREATE TABLE #Product 
(ProductId bigint)


INSERT INTO #Product
SELECT ProductId
FROM [mst].[tbl_Product]
WHERE ProductNAME IN 
(
 'DOPAMINE 200MG HCL {NEON}INJ.'
 ,'DOXIUM TAB.'
,'FORECOX TAB.'
)

IF OBJECT_ID('tempdb..#ClinicalNote') IS NOT NULL
DROP TABLE #ClinicalNote

CREATE TABLE #ClinicalNote
(ID INT IDENTITY(1,1),
ClinicalNote VARCHAR(1000)
)


INSERT INTO #ClinicalNote
(ClinicalNote)
VALUES 
( 'We have kept the patient under observation. We will keep monitoring the patient.')
,('The doctor has observed the patient. He will see the results and monitor him again after 10 hours.')
,('The patient is undergoing good progress. But still we have to observe him for 24 hours.')
,('The patient has been given the medication and services have been rendered.')
,('Drugs have been administered to the patient. We will see how he is reacting to the medication.')


IF OBJECT_ID('TEMPDB..#Package') IS NOT NULL
BEGIN 
DROP TABLE #Package
END


CREATE TABLE #Package
(IPPackageHeaderID BIGINT, 
ServiceId BIGINT, 
IPPackageNoofDays INT, 
[IPPackageSponsorHeaderId] BIGINT )


WHILE(@StartDate <= @EndDate)
BEGIN
SET @RsCount = @RsDailyCount
WHILE(@RsCount > 0 )
BEGIN

/**********************************************Get Patient info by payment type**********************************************/
IF (@PaymentType = 'SELF PAYING' )
BEGIN
SET @PatientId = ((SELECT TOP 1 P.PatientId 
	FROM Patient P
	INNER JOIN mst.PaymentType PT
	ON P.PaymentTypeId = PT.PaymentTypeId
	AND P.IsActive = 1
	AND PT.IsActive = 1
	LEFT JOIN ADMISSION A
	ON A.PatientId = P.PatientId
	WHERE PT.PaymentType = @PaymentType
	AND (A.PatientId IS NULL OR A.DischargeDate IS NOT NULL) 
	ORDER BY NEWID()))

	INSERT INTO #Package
	SELECT P.IPPackageHeaderID, P.ServiceId, p.IPPackageNoofDays, NULL
	FROM [dbo].[tbl_IPPackageHeader] P 
	INNER JOIN mst.Service S ON S.ServiceId = P.ServiceId
	AND ServiceName IN (
	  'BILATERAL HEART STUDIES PACKAGE'
	, 'AKUHN NORMAL DELIVERY PACKAGE GP'
	, 'PACKAGE THYROIDECTOMY LOBECTOMY GP'
	) AND P.IsActive = 1 AND S.IsActive = 1

END
IF (@PaymentType = 'SPONSOR' )
BEGIN
SET @PatientId = ((SELECT TOP 1 P.PatientId 
	FROM Patient P
	INNER JOIN mst.PaymentType PT
	ON P.PaymentTypeId = PT.PaymentTypeId
	AND P.IsActive = 1
	AND PT.IsActive = 1
	INNER JOIN PatientSponsor PS 
	ON PS.PatientId = P.PatientId AND PS.IsActive = 1
	LEFT JOIN ADMISSION A
	ON A.PatientId = P.PatientId
	WHERE PT.PaymentType = @PaymentType
	AND (A.PatientId IS NULL OR A.DischargeDate IS NOT NULL) 
	ORDER BY NEWID()))

	INSERT INTO #Package
	SELECT NULL, P.ServiceId, p.IPPackageNoofDays, P.IPPackageSponsorHeaderId
	FROM [mst].[tbl_IPPackageSponsorHeader] P 
	INNER JOIN mst.Service S ON S.ServiceId = P.ServiceId
	AND ServiceName IN (
	  'BILATERAL HEART STUDIES PACKAGE'
	, 'AKUHN NORMAL DELIVERY PACKAGE GP'
	, 'PACKAGE THYROIDECTOMY LOBECTOMY GP'
	) AND P.IsActive = 1 AND S.IsActive = 1


	
END
/**********************************************Insert Reservation Details**********************************************/
  INSERT INTO [dbo].[Reservation]
(	   [PatientId]
      ,[ReservationTypeId]
      ,[BillingClassId]
      ,[PaymentTypeId]
      ,[EntitlementId]
      ,[TreatmentTypeId]
      ,[CreatedById]
      ,[UpdatedById]
      ,[CreatedTime]
      ,[LastUpdatedTime]
      ,[IsActive]
      ,[HospitalId]
      ,[AdmissionReasonId]
      ,[ExpectedAdmissionDate]
      ,[ExpectedLOS]
      ,[AdmittingConsultantId]
      ,[DiagnosisId]
      ,[ICDId]
      ,[ReservationRemarks]
      ,[ReservationStatusId]
      ,[ReservationNumber]
      ,[CancelledById]
      ,[CancelReasonId]
      ,[CancelRemark]
      ,[CancellationSendSMS]
      ,[CancellationSendMail]
      ,[ReservationModifyReasonId]
      ,[ModifyRemark]
      ,[AdmissionNumber])
SELECT P.[PatientId]
      ,RT.[ReservationTypeId]
      ,PC.PatientClassId
      ,PT.[PaymentTypeId]
      ,PS.[EntitlementId]
      ,TT.[TreatmentTypeId]
      ,@UserId
      ,@UserId
      ,@StartDate
      ,@StartDate
      ,1
      ,1
      ,AR.[AdmissionReasonId]
      ,DATEADD(DD, @expadmdays , @STARTDATE)  AS [ExpectedAdmissionDate]
      ,CAST(RAND()*150 AS INT )%@ExpectedLOS + 1 AS [ExpectedLOS]
      ,D.DoctorId AS [AdmittingConsultantId]
      ,DG.[DiagnosisId] AS [DiagnosisId]
      ,DG.ICDId AS [ICDId]
      ,'Please come to the hospital with all relevant documents' AS [ReservationRemarks]
      ,RS.[ReservationStatusId] AS [ReservationStatusId]
      ,NULL AS [ReservationNumber]
      ,NULL AS [CancelledById]
      ,NULL AS [CancelReasonId]
      ,NULL AS [CancelRemark]
      ,NULL AS [CancellationSendSMS]
      ,NULL AS [CancellationSendMail]
      ,NULL AS [ReservationModifyReasonId]
      ,NULL AS [ModifyRemark]
      ,NULL AS [AdmissionNumber]

FROM Patient P 
LEFT JOIN (SELECT TOP 1 ReservationTypeId FROM mst.ReservationType WHERE IsActive = 1 ORDER BY NEWID()) RT
ON 1 = 1
LEFT JOIN (SELECT TOP 1 PatientClassId FROM mst.PatientClass WHERE IsActive = 1 AND PatClassAppl = 'I' ORDER BY NEWID()) PC
ON 1 = 1
INNER JOIN mst.PaymentType PT
ON PT.PaymentType = @PaymentType
LEFT JOIN PatientSponsor PS
ON PS.PatientId = P.PatientId
AND PS.IsActive = 1
INNER JOIN (SELECT TOP 1 TreatmentTypeId FROM mst.TreatmentType WHERE IsActive = 1 AND TreatmentType = @TreatmentType ORDER BY NEWID()) TT
ON 1 = 1
INNER JOIN (SELECT TOP 1 AdmissionReasonId FROM [mst].[AdmissionReason] WHERE IsActive = 1 ORDER BY NEWID()) AR
ON 1 = 1
INNER JOIN (SELECT DoctorId FROM [mst].[Doctor] WHERE @DoctorId = DoctorId AND IsActive = 1 ) D
ON 1 = 1
INNER JOIN (SELECT TOP 1 DiagnosisId, ICDId FROM [mst].[Diagnosis] WHERE IsActive = 1 ORDER BY NEWID()) DG
ON 1 = 1
INNER JOIN (SELECT ReservationStatusId FROM [mst].[ReservationStatus] WHERE ReservationStatus = 'Admitted' and IsActive = 1 ) RS
ON 1 = 1
WHERE P.PatientId = @PatientId

SET @ReservationNumber = @@IDENTITY

if @ReservationNumber  is null 
begin 
RAISERROR (N'Reservation %s %d.', -- Message text.  
           16, -- Severity,  
           1, -- State,  
           @ReservationNumber -- First argument.  
           ); -- Second argument.  
-- The message text returned is: This is message number 5. 
end

/**********************************************Update ReservationNumber**********************************************/

UPDATE Reservation
SET ReservationNumber = @ReservationNumber
WHERE ReservationId = (SELECT MAX(ReservationId) FROM Reservation ) 


/**********************************************Insert treatment Details**********************************************/
IF EXISTS (SELECT 1 FROM Reservation RS
		   INNER JOIN mst.TreatmentType TT
		   ON TT.TreatmentTypeId = RS.TreatmentTypeId
		   AND ReservationNumber = @ReservationNumber
		   AND TT.TreatmentType = 'Medical'
		   )
BEGIN
INSERT INTO [dbo].[ReservationMedicalTreatment]
([ReservationId]
      ,[ProcedureId]
      ,[AdmissionProcedureDate]
      ,[CreatedById]
      ,[UpdatedById]
      ,[CreatedTime]
      ,[LastUpdatedTime]
      ,[IsActive]
      ,[DeleteComments]
      ,[HospitalId]
      ,[IsOTBillingDetailsCaptured])
	  
	  SELECT RS.ReservationId
	  , PR.ProcedureId
	  , DATEADD(DD, CAST(RAND()*100 AS INT)%2, RS.ExpectedAdmissionDate) AS [AdmissionProcedureDate]
	  , 1, 1, Getdate(), Getdate(), 1, NULL , 1, NULL 
	  FROM Reservation RS 
	  INNER JOIN (SELECT TOP 1 ProcedureId FROM [mst].TreatmentProcedure WHERE IsActive = 1 ORDER BY NEWID()) PR
	  ON 1 = 1
	  WHERE ReservationNumber = @ReservationNumber
	  
END

ELSE 
BEGIN

INSERT INTO [dbo].[ReservationOTBooking]
(	   [ReservationId]
      ,[TheatreTypeId]
      ,[TheatreId]
      ,[AnaesthesiaTypeId]
      ,[BloodGroupId]
      ,[BookingDate]
      ,[BookingFromTime]
      ,[BookingToTime]
      ,[CreatedById]
      ,[UpdatedById]
      ,[CreatedTime]
      ,[LastUpdatedTime]
      ,[IsActive]
      ,[DeleteComments]
      ,[HospitalId])
SELECT [ReservationId]
      ,TT.[TheatreTypeId]
      ,TT.OperationTheatreId
      ,TT.AnaesthesiaTypeId AS [AnaesthesiaTypeId]
      ,ISNULL(P.[BloodGroupId], 1) 
      ,DATEADD(DAY, 1+CAST( RAND()*9090 AS INT)%2, RS.[ExpectedAdmissionDate]) 
      ,TT.FromTime AS [BookingFromTime]
      ,TT.ToTime AS [BookingToTime]
      ,1 AS [CreatedById]
      ,1 AS [UpdatedById]
      ,GETDATE() AS [CreatedTime]
      ,GETDATE() AS [LastUpdatedTime]
      ,1 AS [IsActive]
      ,NULL AS [DeleteComments]
      ,1 AS [HospitalId]
FROM Reservation RS 
INNER JOIN 
	(SELECT TOP 1 TT.TheatreTypeId , T.OperationTheatreId, T.FromTime, T.ToTime, AT.AnaesthesiaTypeId, WD.WeekDay
	FROM [mst].TheatreType  TT
	INNER JOIN [mst].[tbl_OperationTheatre] OT
	ON TT.TheatreTypeId = OT.TheatreTypeId
	INNER JOIN mst.Theatre T 
	ON OT.OperationTheatreId = T.OperationTheatreId
	AND TT.IsActive = 1 
	AND OT.IsActive = 1 
	AND T.IsActive = 1 
	INNER JOIN mst.AnaesthesiaType AT
	ON AT.IsActive = 1
	INNER JOIN mst.WeekDay WD
	ON WD.IsActive = 1
	AND T.WeekDayID = WD.WeekDayId
	INNER JOIN Reservation RS ON RS.ReservationNumber = @ReservationNumber
	AND WD.WeekDay = DATENAME(DW, RS.ExpectedAdmissionDate)
	ORDER BY NEWID()
	) TT
ON 1 = 1 
AND ReservationNumber = @ReservationNumber
INNER JOIN Patient P 
ON P.PatientId = RS.PatientId



INSERT INTO [dbo].[ReservationSurgeryDetail]
(	   [ReservationSurgeryDetailId]
      ,[SurgeryId]
      ,[DepartmentId]
      ,[PrimarySurgeonId]
      ,[GradeId]
      ,[FromTime]
      ,[ToTime]
      ,[Remark]
      ,[CreatedById]
      ,[UpdatedById]
      ,[CreatedTime]
      ,[LastUpdatedTime]
      ,[IsActive]
      ,[DeleteComments]
      ,[HospitalId]
      ,[IsPrimarySurgery]
      ,[IsOTBillingDetailsCaptured])
	  
SELECT RS.ReservationId
	  ,SR.SurgeryId
	  ,SR.DepartmentId
	  ,SR.DoctorId
	  ,SR.GradeId
	  ,ROTB.BookingFromTime
	  ,ROTB.BookingToTime
	  ,'' AS REMARK
	  ,@UserId
	  ,@UserId
	  ,@StartDate
	  ,@StartDate
	  ,1
	  ,NULL
	  ,1
	  ,1
	  ,0
	   
FROM Reservation RS 
INNER JOIN (SELECT TOP 1 S.SurgeryId, D.DepartmentId , D.DoctorId, SG.GradeId
			FROM [mst].Surgery S 
			INNER JOIN [mst].Doctor D
			ON D.DepartmentId = S.DepartmentId
			AND D.IsActive = 1 AND S.IsActive = 1
			INNER JOIN [mst].[tbl_SurgeryGradeMapping] SG
			ON SG.SurgeryId = S.SurgeryId
			AND SG.IsActive = 1
			ORDER BY NEWID()) SR
ON ReservationNumber = @ReservationNumber
INNER JOIN [dbo].[ReservationOTBooking] ROTB
ON ROTB.ReservationId = RS.ReservationId

END

/*Bed alloaction for this reservation*/
UPDATE B
SET ReservationId = R.ReservationId, BedStatusCode = 'A'
FROM Reservation R 
INNER JOIN (
	SELECT TOP 1 BEDID,ReservationId,BedStatusCode 
	from mst.BED B
	INNER JOIN mst.Ward W 
	ON B.WardId = B.WardId
	AND W.WardName = @WardName 
	WHERE BedStatusCode LIKE 'V' AND B.IsActive = 1 AND W.IsActive = 1
	ORDER BY NEWID()) B
ON 1 = 1
WHERE  R.ReservationNumber = @ReservationNumber

/*Create Admission for this reservation */
/**/
INSERT INTO [dbo].[Admission]
	([PatientId]
	,[BillingClassId]
	,[PaymentTypeId]
	,[TreatmentTypeId]
	,[AdmissionReasonId]
	,[AdmissionModeId]
	,[AdmittingConsultantId]
	,[TreatingConsultantId]
	,[JointConsultantId]
	,[DiagnosisId]
	,[ICDId]
	,[AdmissionKnownToId]
	,[AdmissionRemarks]
	,[BedId]
	,[ExpectedDischargeDate]
	,[ExpectedLOS]
	,[DietId]
	,[CreatedById]
	,[UpdatedById]
	,[CreatedTime]
	,[LastUpdatedTime]
	,[IsActive]
	,[DeleteComments]
	,[HospitalId]
	,[AdmissionNumber]
	,[MotherAdmissionNumber]
	,[AdmissionTypeCode]
	,[ChiefComplaint]
	,[AdmissionKnownToName]
	,[AdmissionDate]
	,[DischargeDate]
	,[EntitlementId]
	,[InstituteId]
	,[ReferenceFile]
	,[ConcessionRemarks]
	,[CurrentBedId]
	,[DischargeStatusId]
	,[EligibleBillingClassId]
	,[FolioRemarks])
SELECT 
	 R.[PatientId]
	,R.[BillingClassId]
	,R.[PaymentTypeId]
	,R.[TreatmentTypeId]
	,R.[AdmissionReasonId]
	,AM.[AdmissionModeId]
	,R.[AdmittingConsultantId]
	,R.AdmittingConsultantId AS [TreatingConsultantId]
	,NULL AS [JointConsultantId]
	,R.[DiagnosisId]
	,R.[ICDId]
	,AKT.[AdmissionKnownToId]
	,'Submitted documents are fine and we can proceed.' AS [AdmissionRemarks]
	,B.[BedId]
	,DATEADD(HH, CAST(RAND()*900 AS INT)%12+24*R.[ExpectedLOS], CAST(R.ExpectedAdmissionDate AS DATETIME) )  AS [ExpectedDischargeDate]
	,R.[ExpectedLOS]
	,NULL AS [DietId]
	,1 AS [CreatedById]
	,1 AS [UpdatedById]
	,GETDATE() AS [CreatedTime]
	,GETDATE() AS [LastUpdatedTime]
	,1 AS [IsActive]
	,NULL AS [DeleteComments]
	,1 AS [HospitalId]
	,NULL AS [AdmissionNumber]
	,NULL AS [MotherAdmissionNumber]
	,ATC.[AdmissionTypeCode]
	,CC.[ChiefComplaint]
	,NULL AS [AdmissionKnownToName]
	,R.ExpectedAdmissionDate AS [AdmissionDate]
	,NULL AS [DischargeDate]
	,R.[EntitlementId]
	,ENT.[InstituteId]
	,NULL AS [ReferenceFile]
	,NULL AS [ConcessionRemarks]
	,B.BedId AS [CurrentBedId]
	,NULL AS [DischargeStatusId]
	,NULL AS [EligibleBillingClassId]
	,NULL AS [FolioRemarks]
FROM Reservation R
INNER JOIN (SELECT TOP 1 AdmissionModeId FROM mst.AdmissionMode WHERE IsActive = 1 ORDER BY NEWID()) AM
ON 1 = 1
INNER JOIN mst.AdmissionType ATC ON ATC.AdmissionTypeCode = 'PA' AND ATC.IsActive = 1
INNER JOIN (SELECT TOP 1 AdmissionKnownToId, AdmissionKnownTo FROM [mst].[AdmissionKnownTo] WHERE IsActive = 1 ORDER BY NEWID()) AKT
ON 1 = 1
INNER JOIN mst.Bed B 
ON B.ReservationId = R.ReservationId
INNER JOIN (SELECT TOP 1 Description AS ChiefComplaint FROM ChiefComplaint WHERE IsActive = 1 AND Description = @ChiefComplaint ORDER BY NEWID()) CC
ON 1 = 1
LEFT JOIN Entitlement ENT 
ON ENT.EntitlementId = R.EntitlementId
WHERE ReservationNumber = @ReservationNumber


SELECT @AdmissionId = @@IDENTITY
Print ('Admission is'+ Cast((@AdmissionId) as varchar(100)))


if @AdmissionId is null 
begin 
RAISERROR (N'admission %s %d.', -- Message text.  
           16, -- Severity,  
           1, -- State,  
           @AdmissionId -- First argument.  
	       ); -- Second argument.  
-- The message text returned is: This is message number 5.  

end


SELECT @DischargeDateTime = ( DATEADD(HH, 14 + 24*CAST(RAND()*100 AS INT )%3 ,CAST (A.ExpectedDischargeDate AS DATETIME))) 
FROM Admission A
WHERE AdmissionId = @AdmissionId

UPDATE Admission
SET AdmissionNumber = @AdmissionId
WHERE AdmissionId = @AdmissionId

--SELECT @AdmissionId AS ADMISSIONID, @ReservationNumber AS RESNUMBER

UPDATE Reservation 
SET AdmissionNumber = @AdmissionId
WHERE ReservationNumber = @ReservationNumber 

SELECT @AdmissionDate = CAST(AdmissionDate AS DATE ) 
FROM Admission
WHERE AdmissionId = @AdmissionId




/*INSERT INTO [dbo].[tbl_IPPatientClinicalNote]
([AdmissionId]
,[ClinicalNote]
,[HospitalId]
,[CreatedById]
,[UpdatedById]
,[CreatedTime]
,[LastUpdatedTime]
,[IsActive])
SELECT TOP 5 @AdmissionId
,[ClinicalNote]
,1
,1
,1
,DATEADD(DD, ID, @AdmissionDate)
,DATEADD(DD,ID , @AdmissionDate)
,1 
FROM #ClinicalNote
ORDER BY ID*/


INSERT INTO [dbo].[tbl_IPPatientVitals]
	([AdmissionId]
	,[PatientId]
	,[RegistrationNumber]
	,[VitalSignId]
	,[Value]
	,[Remark]
	,[CreatedById]
	,[CreatedTime]
	,[UpdatedById]
	,[LastUpdatedTime]
	,[IsActive]
	,[HospitalId]
	,[VitalSignGroupId]
	,[CapturingDate]
	,[TriageDetailId])
SELECT TOP 10  @AdmissionId
	,P.[PatientId]
	,P.[RegistrationNumber]
	,VSGM.[VitalSignId]
	,MinValue+(CAST(RAND() *900 AS INT)%CAST(MaxValue - MinValue AS INT))
	,NULL
	,1
	,DATEADD(HH, CAST(RAND()*900 AS INT)%5, @AdmissionDate)
	,1
	,DATEADD(HH, CAST(RAND()*900 AS INT)%5, @AdmissionDate)
	,1
	,1
	,VSG.[VitalSignGroupId]
	,DATEADD(HH, CAST(RAND()*900 AS INT)%5, @AdmissionDate)
	,[TriageDetailId]
FROM MST.tbl_VitalSign VS
INNER JOIN Admission A
ON A.AdmissionId = @AdmissionId
INNER JOIN Patient P 
ON P.PatientId = A.PatientId
INNER JOIN MST.tbl_VitalSignGroup VSG 
ON VSG.VitalSignGroupCode = 'GEN'
INNER JOIN MST.tbl_VitalSignGroupMap VSGM
ON VSGM.VitalSignGroupId = VSG.VitalSignGroupId
AND VS.VitalSignId = VSGM.VitalSignId


UPDATE  B
SET BedCurrentAdmissionNumber = A.AdmissionNumber, BedCurrentPatientId = A.PatientId, BedHoldingPatientId = A.PatientId, PatientBillingClassId = A.BillingClassId, 
BedStatusCode = 'C', BedCurrentGender = case when g.GenderCode = 'Male' then 'M' else 'F' end
FROM Admission A 
INNER JOIN Reservation R 
ON R.ReservationNumber = @ReservationNumber
INNER JOIN mst.BED B
ON B.ReservationId = R.ReservationId
INNER JOIN Patient P 
ON P.PatientId = A.PatientId
INNER JOIN mst.Gender G 
ON G.GenderId = P.GenderId
WHERE AdmissionId = @AdmissionId


EXEC [dbo].[usp_AutoPostServices] @doctorid, @AdmissionId, 1, 1

/**************************************************** Add package to admission****************************************************************/

INSERT INTO [dbo].[tbl_IPPackageTransfer]
  ([AdmissionId]
      ,[IPPackageHeaderID]
      ,[PackageStartTime]
      ,[PackageEndTime]
      ,[ReasonId]
      ,[Remark]
      ,[CreatedById]
      ,[CreatedTime]
      ,[HospitalId]
      ,[IsActive]
      ,[IPPackageSponsorHeaderId]
      ,[ServiceId])

SELECT @AdmissionId, IPPackageHeaderID, AdmissionDate, DATEADD(DD, P.IPPackageNoofDays, AdmissionDate ), NULL, 'NA', 1, AdmissionDate, 1, 1, IPPackageSponsorHeaderId, ServiceId
FROM #Package P 
INNER JOIN Admission A
ON A.AdmissionId = @AdmissionId

SELECT @IPPackageTransferId = @@IDENTITY

INSERT INTO [dbo].[tbl_IPPackageTransferDetails]
	([IPPackageTransferId]
	,[IPPackageBillProperty]
	,[PackageQuantity]
	,[PackageCost]
	,[IPPackageCategoryId]
	,[IPPackageType]
	,[IPPackageCategory]
	,[SubCategoryId]
	,[BedGroupId]
	,[CreatedById]
	,[CreatedTime]
	,[IsActive]
	,[HospitalId])

SELECT 
	[IPPackageTransferId]
	,ISNULL(PHD.[IPPackageBillProperty],SPHD.[IPPackageBillProperty] )
	,ISNULL(PHD.[PackageQuantity]	   ,SPHD.[PackageQuantity]		)
	,ISNULL(PHD.[PackageCost]		   ,SPHD.[PackageCost]			)
	,ISNULL(PHD.[IPPackageCategoryId] ,SPHD.[IPPackageCategoryId]	)
	,ISNULL(PHD.[IPPackageType]		 ,SPHD.[IPPackageType]			)
	,ISNULL(PHD.[IPPackageCategory]	 ,SPHD.[IPPackageCategory]		)
	,ISNULL(PHD.[SubCategoryId]		 ,SPHD.[SubCategoryId]			)
	,ISNULL(PHD.[BedGroupId]		 ,SPHD.[BedGroupId]				)
	,1
	,A.AdmissionDate
	,1
	,1
FROM [dbo].[tbl_IPPackageTransfer] PT 
LEFT JOIN [dbo].[tbl_IPPackageHeader] PH 
ON PT.IPPackageHeaderID = PH.IPPackageHeaderID
LEFT JOIN [dbo].[tbl_IPPackageMasterDetails] PHD
ON PHD.IPPackageHeaderID = PH.IPPackageHeaderID
INNER JOIN Admission A 
ON A.AdmissionId = @AdmissionId 
LEFT JOIN [mst].[tbl_IPPackageSponsorHeader] SPH
ON SPH.IPPackageSponsorHeaderId = PT.IPPackageSponsorHeaderId
LEFT JOIN [mst].[tbl_IPPackageSponsorDetails] SPHD
ON SPHD.IPPackageSponsorHeaderId = SPH.IPPackageSponsorHeaderId
WHERE PT.IPPackageTransferId =  @IPPackageTransferId

/*************************************************Insert IP Service orders*******************************************************************/

INSERT INTO [dbo].[tbl_IPServiceRequestHeader]
	([OrderNumber]
	,[PatientId]
	,[RegistrationNumber]
	,[AdmissionId]
	,[AdmissionNumber]
	,[RequestedDateTime]
	,[IsBilled]
	,[CurrentAdmittingDoctorId]
	,[PatientBillingClassId]
	,[Remarks]
	,[InvoiceNumber]
	,[PatientTaxAmount]
	,[SponsorTaxAmount]
	,[PatientEmergencyAmount]
	,[SponsorEmergencyAmount]
	,[IsNSIRequest]
	,[CreatedById]
	,[UpdatedById]
	,[CreatedTime]
	,[LastUpdatedTime]
	,[HospitalId]
	,[OTAuthorizerID])

SELECT SG.Prefix+'-'+ CAST (NEXT VALUE FOR [dbo].[seq_IPRequestOrderNumber] AS varchar(10))+'-'+SG.Suffix
	,P.[PatientId]
	,P.[RegistrationNumber]
	,A.[AdmissionId]
	,A.[AdmissionNumber]
	,dateadd(dd, cast(rand()*100*P.[PatientId] as int )%3+1 , a.AdmissionDate)
	,0 
	,A.AdmittingConsultantId
	,A.BillingClassId
	,NULL
	,NULL
	,NULL
	,NULL
	,NULL
	,NULL
	,0
	,1
	,1
	,dateadd(hh, cast(rand()*100*P.[PatientId] as int )%3+1 , a.AdmissionDate)
	,NULL
	,1
	,NULL
FROM mst.SequenceGenerator SG
INNER JOIN Admission A 
ON A.AdmissionId = @AdmissionId 
INNER JOIN Patient P 
ON P.PatientId = A.PatientId
WHERE SG.SequenceName = 'IP SERVICE ORDER'

SELECT @IPServiceRequestHeaderId =  @@IDENTITY

/*************************************Insert IP Service Order Details************************************/

 
INSERT INTO [dbo].[tbl_IPServiceRequestDetails]
	([PatientId]
	,[RegistrationNumber]
	,[AdmissionId]
	,[AdmissionNumber]
	,[IPServiceRequestHeaderId]
	,[OrderNumber]
	,[STAT]
	,[ServiceId]
	,[BedId]
	,[IssuedQuantity]
	,[IsBedService]
	,[CreatedById]
	,[CreatedTime]
	,[HospitalId]
	,[Price]
	,[Remarks]
	,[ServiceRequestDate]
	,[BloodGroupId]
	,[ServiceArea]
	,PatientBillingClassId
	,HospitalTariff
	,BedBillingClassId
	,[IsServiceRenderred])
SELECT IPH.[PatientId]
	,IPH.[RegistrationNumber]
	,IPH.[AdmissionId]
	,IPH.[AdmissionNumber]
	,IPH.[IPServiceRequestHeaderId]
	,IPH.[OrderNumber]
	,'N'
	,S.[ServiceId]
	,A.[BedId]
	,1
	,0
	,1
	,IPH.[RequestedDateTime]
	,1
	,TR.ServiceTariff
	,'NA'
	,IPH.[RequestedDateTime]
	,P.[BloodGroupId]
	,'AUTO'
	,A.BillingClassId
	,TR.ServiceTariff
	,BED.BillingClassId
	,CASE WHEN S.ServiceDefaultRender = 'Y' THEN 1 ELSE 0 END
FROM [dbo].[tbl_IPServiceRequestHeader] IPH
INNER JOIN Admission A 
ON A.AdmissionId = @AdmissionId 
INNER JOIN mst.Bed BED
ON BED.BedId = A.BedId
INNER JOIN (
	SELECT TOP 10 ServiceId, ServiceDefaultRender FROM #Service ORDER BY NEWID()
	--UNION 
	--SELECT top 5 S.ServiceID, S.ServiceDefaultRender
	--FROM tbl_IPPackageTransfer PT
	--INNER JOIN Service S 
	--ON S.ServiceId = PT.ServiceId
	--WHERE IPPackageTransferId= @IPPackageTransferId
	--ORDER BY NEWID()
	) S
ON 1 = 1
INNER JOIN [dbo].[Patient] P
ON P.PatientId = A.PatientId
INNER JOIN [dbo].[Tariff] TR
ON TR.ServiceId = S.ServiceId
AND TR.IsActive = 1
AND TR.ServiceTariffPatClassId = A.BillingClassId

WHERE IPServiceRequestHeaderId = @IPServiceRequestHeaderId



/***************************************************Lab & Rad Transaction Start***********************************************/





DECLARE 
 @OrderId BIGINT
,@PatientClassCode Varchar(10) = 'PAT'
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
	SELECT ORDERNUMBER
		,patientid
		,CurrentAdmittingDoctorId
		,null
		,[RequestedDateTime]
		,1,1,[RequestedDateTime], [RequestedDateTime],1,NULL, 1, AdmissionNumber, NULL, NULL, NULL, NULL, CurrentAdmittingDoctorId, 0
		FROM tbl_IPServiceRequestHeader
		WHERE IPServiceRequestHeaderId = @IPServiceRequestHeaderId


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
		,IPD.ServiceId
		,1,1,IPD.ServiceRequestDate, IPD.ServiceRequestDate, 1, NULL, 1,SEC.DepartmentId ,SEC.SectionId, 1, 'N', 1, NULL
	FROM tbl_IPServiceRequestDetails IPD
	INNER JOIN mst.Service S 
	ON S.ServiceId = IPD.ServiceId
	AND IPServiceRequestHeaderId = @IPServiceRequestHeaderId
	INNER JOIN mst.Section SEC
	ON SEC.SectionId = s.SectionId
	INNER JOIN mst.Department D 
	ON D.DepartmentId = SEC.DepartmentId
	AND D.DepartmentCode IN ('LAB', 'RAD')


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
	  


/***************************************************Lab & Rad Transaction End ***********************************************/


/************************************************Daily posting services********************************************************/

WHILE(cast(@AdmissionDate as date) < = cast (@DischargeDateTime as date ) )
BEGIN

	INSERT INTO [dbo].[tbl_IPServiceRequestDetails]
	([PatientId]
		  ,[RegistrationNumber]
		  ,[AdmissionId]
		  ,[AdmissionNumber]
		  ,[IPServiceRequestHeaderId]
		  ,[OrderNumber]
		  ,[STAT]
		  ,[ServiceId]
		  ,[BedId]
		  ,[IssuedQuantity]
		  ,[PrescribedDoctorId]
		  ,[IsBedService]
		  ,[CreatedById]
		  ,[UpdatedById]
		  ,[CreatedTime]
		  ,[LastUpdatedTime]
		  ,[HospitalId]
		  ,[Price]
		  ,[Remarks]
		  ,[ServiceRequestDate]
		  ,[RenderDoctorId]
		  ,[ByPercentage]
		  ,[ByTimes]
		  ,[ByAmount]
		  ,[BloodGroupId]
		  ,[ServiceArea]
		  ,PatientBillingClassId
		  ,HospitalTariff
		  ,BedBillingClassId
		  ,[IsServiceRenderred])
	SELECT IPH.[PatientId]
		  ,IPH.[RegistrationNumber]
		  ,IPH.[AdmissionId]
		  ,IPH.[AdmissionNumber]
		  ,IPH.[IPServiceRequestHeaderId]
		  ,IPH.[OrderNumber]
		  ,'N'
		  ,DP.[ServiceId]
		  ,A.CurrentBedId
		  ,1
		  ,NULL
		  ,0
		  ,1
		  ,NULL
		  ,@AdmissionDate
		  ,NULL
		  ,1
		  ,TR.ServiceTariff
		  ,'Auto Post Daily Posting Services'
		  ,@AdmissionDate
		  ,NULL
		  ,NULL
		  ,NULL
		  ,NULL
		  ,P.[BloodGroupId]
		  ,'AUTO'
		  ,A.BillingClassId
		  ,TR.ServiceTariff
		  ,BED.BillingClassId
		  ,CASE WHEN S.ServiceDefaultRender = 'Y' THEN 1 ELSE 0 END
	FROM [dbo].[tbl_IPServiceRequestHeader] IPH
	INNER JOIN Admission A 
	ON A.AdmissionId = @AdmissionId 
	INNER JOIN mst.Bed BED
	ON BED.BedId = A.BedId
	INNER JOIN [mst].[tbl_DailyPostings] DP
	ON DP.IsActive = 1 AND DP.ActiveStatus = 1
	INNER JOIN [dbo].[Patient] P
	ON P.PatientId = A.PatientId
	INNER JOIN [dbo].[Tariff] TR
	ON TR.ServiceId = DP.ServiceId
	AND TR.IsActive = 1
	AND TR.ServiceTariffPatClassId = A.BillingClassId
	INNER JOIN mst.Service S
	ON S.ServiceId = DP.ServiceId
	WHERE IPServiceRequestHeaderId = @IPServiceRequestHeaderId
	UNION ALL
	SELECT IPH.[PatientId]
		  ,IPH.[RegistrationNumber]
		  ,IPH.[AdmissionId]
		  ,IPH.[AdmissionNumber]
		  ,IPH.[IPServiceRequestHeaderId]
		  ,IPH.[OrderNumber]
		  ,'N'
		  ,DSP.[ServiceId]
		  ,A.CurrentBedId
		  ,1
		  ,DSP.DoctorId
		  ,0
		  ,1
		  ,NULL
		  ,@AdmissionDate
		  ,NULL
		  ,1
		  ,TR.ServiceTariff
		  ,'Auto Post Doctor Special Services'
		  ,@AdmissionDate
		  ,NULL
		  ,NULL
		  ,NULL
		  ,NULL
		  ,P.[BloodGroupId]
		  ,'AUTO'
		  ,A.BillingClassId
		  ,TR.ServiceTariff
		  ,BED.BillingClassId
		  ,CASE WHEN S.ServiceDefaultRender = 'Y' THEN 1 ELSE 0 END
	FROM [dbo].[tbl_IPServiceRequestHeader] IPH
	INNER JOIN Admission A 
	ON A.AdmissionId = @AdmissionId 
	INNER JOIN mst.Bed BED
	ON BED.BedId = A.BedId
	INNER JOIN [mst].[tbl_DoctorServicePost] DSP
	ON DSP.IsActive = 1 AND DSP.ActiveStatus = 1 
		AND DSP.DoctorId = A.AdmittingConsultantId
	INNER JOIN [dbo].[Patient] P
	ON P.PatientId = A.PatientId
	INNER JOIN [dbo].[Tariff] TR
	ON TR.ServiceId = DSP.ServiceId
	AND TR.ServiceTariffPatClassId = A.BillingClassId
	AND TR.IsActive = 1
	INNER JOIN mst.Service S 
	ON S.ServiceId = DSP.ServiceId
	WHERE IPServiceRequestHeaderId = @IPServiceRequestHeaderId
	AND (
		DSP.UntilDischarge = 1
		OR DSP.Duration > DATEDIFF(DAY, CAST(A.AdmissionDate AS DATE) , CAST(@AdmissionDate AS DATE) )
		)

SET @AdmissionDate = DATEADD(DD, 1, @AdmissionDate)
END





/*******************************************************Transfer Patient to ICU********************************************************/

IF (@NeedWardTransfer = 1)
BEGIN
INSERT INTO [dbo].[tbl_TransferLocation]
	([PatientId]
	,[AdmissionId]
	,[AdmissionNumber]
	,[CurrentBedId]
	,[RequestingBedId]
	,[BedBillingClassId]
	,[TransferReasonId]
	,[Remarks]
	,[TransferStatusId]
	,[HospitalId]
	,[CreatedById]
	,[UpdatedById]
	,[CreatedTime]
	,[LastUpdatedTime]
	,[IsActive]
	,[DeleteComments]
	,[TransferTypeId]
	,[IsBedRetain]
	,[RequestedDate]
	,[CancelledDate]
	,[ConfirmedDate]
	,[BedAllottedDate]
	,[AllottedBedId]
	,[ConfirmedById]
	,[InConsistentBedReason]
	,[BedDeallocatedDate]
	,[DeallocatedById]
	,[TransferCancelReasonId]
	,[CancelRemark]
	,[ConfirmRemarks]
	,[RetainBedStatus])
SELECT
	 A.[PatientId]
	,A.[AdmissionId]
	,A.[AdmissionNumber]
	,A.[CurrentBedId]
	,BED.BedId AS [RequestingBedId]
	,BED.BillingClassId AS [BedBillingClassId]
	,TR.[TransferReasonId]
	,NULL AS [Remarks]
	,TS.[TransferStatusId]
	,A.[HospitalId]
	,A.[CreatedById]
	,A.[UpdatedById]
	,A.[CreatedTime]
	,A.[LastUpdatedTime]
	,A.[IsActive]
	,NULL AS [DeleteComments]
	,TT.[TransferTypeId]
	,0 AS [IsBedRetain]
	,DATEADD(HH, 5 + CAST(RAND()*9090 AS INT )% 15 , A.AdmissionDate ) AS [RequestedDate]
	,NULL AS [CancelledDate]
	,DATEADD(HH, 20 + CAST(RAND()*9090 AS INT )% 2 , A.AdmissionDate ) AS [ConfirmedDate]
	,DATEADD(HH, 22 + CAST(RAND()*9090 AS INT )% 2 , A.AdmissionDate ) AS [BedAllottedDate]
	,BED.BedId AS [AllottedBedId]
	,1 AS [ConfirmedById]
	,NULL AS [InConsistentBedReason]
	,NULL AS [BedDeallocatedDate]
	,NULL AS [DeallocatedById]
	,NULL AS [TransferCancelReasonId]
	,NULL AS [CancelRemark]
	,'Confirm' AS [ConfirmRemarks]
	,NULL AS [RetainBedStatus]
FROM Admission A 
INNER JOIN 	(
	SELECT [TransferTypeId] 
	FROM [mst].[tbl_TransferType] 
	WHERE TransferTypeCode = 'Loc' AND IsActive = 1) TT
ON  1 = 1
INNER JOIN (
	SELECT TOP 1 BEDID, BillingClassId 
	from mst.BED B
	INNER JOIN mst.Ward W 
	ON W.WardId = B.WardId
	WHERE BedStatusCode LIKE 'V' AND B.IsActive = 1
	AND W.WardName = @IntensiveCareUnit
	ORDER BY NEWID()) Bed
ON 1 = 1
INNER JOIN (
	SELECT [TransferStatusId]
	FROM [mst].[tbl_TransferStatus]
	WHERE TransferStatusCode = 'CF'
	AND IsActive = 1) TS
ON 1 = 1
INNER JOIN (
	SELECT TOP 1 [TransferReasonId]
	FROM [mst].[tbl_TransferReason]
	WHERE IsActive = 1
	ORDER BY NEWID()) TR
ON 1= 1
WHERE AdmissionId = @AdmissionId


UPDATE  B
SET BedCurrentAdmissionNumber = NULL, BedCurrentPatientId = NULL, BedHoldingPatientId = NULL, PatientBillingClassId = NULL,BedStatusCode = 'V', ReservationId = NULL
FROM Admission A 
INNER JOIN mst.BED B
ON B.bedid= a.CurrentBedId
WHERE AdmissionId = @AdmissionId

UPDATE A
SET CurrentBedId = AllottedBedId
FROM tbl_TransferLocation TL 
INNER JOIN Admission A 
ON A.AdmissionId = TL.AdmissionId
WHERE TL.AdmissionId = @AdmissionId


UPDATE  B
SET BedCurrentAdmissionNumber = A.AdmissionNumber, BedCurrentPatientId = A.PatientId,  PatientBillingClassId = A.BillingClassId,BedStatusCode = 'C'
FROM Admission A 
INNER JOIN mst.BED B
ON B.bedid= a.CurrentBedId
WHERE AdmissionId = @AdmissionId

/*******************************************************Transfer Patient back to general ward from ICU********************************************************/
INSERT INTO [dbo].[tbl_TransferLocation]
	([PatientId]
	,[AdmissionId]
	,[AdmissionNumber]
	,[CurrentBedId]
	,[RequestingBedId]
	,[BedBillingClassId]
	,[TransferReasonId]
	,[Remarks]
	,[TransferStatusId]
	,[HospitalId]
	,[CreatedById]
	,[UpdatedById]
	,[CreatedTime]
	,[LastUpdatedTime]
	,[IsActive]
	,[DeleteComments]
	,[TransferTypeId]
	,[IsBedRetain]
	,[RequestedDate]
	,[CancelledDate]
	,[ConfirmedDate]
	,[BedAllottedDate]
	,[AllottedBedId]
	,[ConfirmedById]
	,[InConsistentBedReason]
	,[BedDeallocatedDate]
	,[DeallocatedById]
	,[TransferCancelReasonId]
	,[CancelRemark]
	,[ConfirmRemarks]
	,[RetainBedStatus])
SELECT
	 A.[PatientId]
	,A.[AdmissionId]
	,A.[AdmissionNumber]
	,A.[CurrentBedId]
	,BED.BedId AS [RequestingBedId]
	,BED.BillingClassId AS [BedBillingClassId]
	,TR.[TransferReasonId]
	,NULL AS [Remarks]
	,TS.[TransferStatusId]
	,A.[HospitalId]
	,A.[CreatedById]
	,A.[UpdatedById]
	,A.[CreatedTime]
	,A.[LastUpdatedTime]
	,A.[IsActive]
	,NULL AS [DeleteComments]
	,TT.[TransferTypeId]
	,0 AS [IsBedRetain]
	,DATEADD(HH, 24 + CAST(RAND()*9090 AS INT )% 2 , A.AdmissionDate ) AS [RequestedDate]
	,NULL AS [CancelledDate]
	,DATEADD(HH, 26 + CAST(RAND()*9090 AS INT )% 2 , A.AdmissionDate ) AS [ConfirmedDate]
	,DATEADD(HH, 28 + CAST(RAND()*9090 AS INT )% 2 , A.AdmissionDate ) AS [BedAllottedDate]
	,BED.BedId AS [AllottedBedId]
	,1 AS [ConfirmedById]
	,NULL AS [InConsistentBedReason]
	,NULL AS [BedDeallocatedDate]
	,NULL AS [DeallocatedById]
	,NULL AS [TransferCancelReasonId]
	,NULL AS [CancelRemark]
	,'Confirm' AS [ConfirmRemarks]
	,NULL AS [RetainBedStatus]
FROM Admission A 
INNER JOIN 	(
	SELECT [TransferTypeId] 
	FROM [mst].[tbl_TransferType] 
	WHERE TransferTypeCode = 'Loc' AND IsActive = 1) TT
ON  1 = 1
INNER JOIN (
	SELECT TOP 1 BEDID, BillingClassId 
	from mst.BED B
	INNER JOIN mst.Ward W 
	ON W.WardId = B.WardId
	WHERE BedStatusCode LIKE 'V' AND B.IsActive = 1
	AND W.WardName = @WardName
	ORDER BY NEWID()) Bed
ON 1 = 1
INNER JOIN (
	SELECT [TransferStatusId]
	FROM [mst].[tbl_TransferStatus]
	WHERE TransferStatusCode = 'CF'
	AND IsActive = 1) TS
ON 1 = 1
INNER JOIN (
	SELECT TOP 1 [TransferReasonId]
	FROM [mst].[tbl_TransferReason]
	WHERE IsActive = 1
	ORDER BY NEWID()) TR
ON 1= 1
WHERE AdmissionId = @AdmissionId

UPDATE  B
SET BedCurrentAdmissionNumber = NULL, BedCurrentPatientId = NULL, BedHoldingPatientId = NULL, PatientBillingClassId = NULL,BedStatusCode = 'V'
FROM Admission A 
INNER JOIN mst.BED B
ON B.bedid= a.CurrentBedId
WHERE AdmissionId = @AdmissionId

UPDATE Admission
SET CurrentBedId = TL.AllottedBedId
FROM (SELECT TOP 1 AllottedBedId FROM tbl_TransferLocation WHERE AdmissionId = @AdmissionId AND IsActive = 1 ORDER BY TransferLocationId DESC) TL 
WHERE AdmissionId = @AdmissionId

UPDATE  B
SET BedCurrentAdmissionNumber = A.AdmissionNumber, BedCurrentPatientId = A.PatientId,  PatientBillingClassId = A.BillingClassId,BedStatusCode = 'C'
FROM Admission A 
INNER JOIN mst.BED B
ON B.bedid= a.CurrentBedId
WHERE AdmissionId = @AdmissionId

END

/*******************************************************bed & package bill calc*************************************************************************************/
EXEC [dbo].[usp_CalcBedCharges_2] @admissionid , null , null, 1, 1

EXEC [dbo].[usp_CalcPackageCharges] @admissionid
/*************************************************************Invoice Generation**************************************************************************/

/*************************************************************IP Patient Medication order**************************************************************/


INSERT INTO [dbo].[tbl_IPServiceRequestHeader]
	([OrderNumber]
	,[PatientId]
	,[RegistrationNumber]
	,[AdmissionId]
	,[AdmissionNumber]
	,[RequestedDateTime]
	,[IsBilled]
	,[CurrentAdmittingDoctorId]
	,[PatientBillingClassId]
	,[Remarks]
	,[InvoiceNumber]
	,[PatientTaxAmount]
	,[SponsorTaxAmount]
	,[PatientEmergencyAmount]
	,[SponsorEmergencyAmount]
	,[IsNSIRequest]
	,[CreatedById]
	,[UpdatedById]
	,[CreatedTime]
	,[LastUpdatedTime]
	,[HospitalId]
	,[OTAuthorizerID])

SELECT SG.Prefix+'-'+ CAST (NEXT VALUE FOR [dbo].[seq_IPRequestOrderNumber] AS varchar(10))+'-'+SG.Suffix
	,P.[PatientId]
	,P.[RegistrationNumber]
	,A.[AdmissionId]
	,A.[AdmissionNumber]
	,dateadd(dd, cast(rand()*100*P.[PatientId] as int )%3+1 , a.AdmissionDate)
	,0 
	,A.AdmittingConsultantId
	,A.BillingClassId
	,NULL
	,NULL
	,NULL
	,NULL
	,NULL
	,NULL
	,0
	,1
	,1
	,dateadd(HH, cast(rand()*100*P.[PatientId] as int )%3+1 , a.AdmissionDate)
	,dateadd(HH, cast(rand()*100*P.[PatientId] as int )%3+1 , a.AdmissionDate)
	,1
	,NULL
FROM mst.SequenceGenerator SG
INNER JOIN Admission A 
ON A.AdmissionId = @AdmissionId 
INNER JOIN Patient P 
ON P.PatientId = A.PatientId
WHERE SG.SequenceCode = 'IPDR'


SELECT @IPServiceRequestHeaderId =  @@IDENTITY


INSERT INTO [dbo].[tbl_IPPatientMedication]
           ([AdmissionId]
           ,[Days]
           ,[RequestedQuantity]
           ,[CreatedById]
           ,[UpdatedById]
           ,[HospitalId]
           ,[DrugName]
           ,[DrugRequestType]
           ,[IsApprovalRequired]
           ,[ApprovedByID]
           ,[ApprovedQuantity]
           ,[IssuedQuantity]
           ,[Dosage]
           ,[IPServiceRequestHeaderId]
           ,[ApprovalStatus]
           ,[ApprovalReasonId]
           ,[ProductId]
           ,[IsCancelled]
           ,[PendingQuantity]
		   ,CreatedTime
		   )

SELECT top 10 AdmissionId,
  3,
  2,
  1,
  1,
  1,
  ProductName,
  'General',
  1,
  1,
  @DrugApprovedQuantity,
  @DrugIssuedQuantity,
  1,
  IPServiceRequestHeaderId,
  'A',
  (SELECT TOP 1 ApprovalReasonId FROM mst.tbl_ApprovalReason ORDER BY NEWID()),
  P.ProductId,
  0,
  0,
  IPH.RequestedDateTime
  FROM mst.tbl_Product P 
  INNER JOIN tbl_IPServiceRequestHeader IPH 
  ON IPH.[IPServiceRequestHeaderId] = @IPServiceRequestHeaderId
  INNER JOIN #Product PT
  ON PT.ProductId = P.ProductId

SELECT @DrugCount = count(1) 
FROM [dbo].[tbl_IPPatientMedication] WHERE IPServiceRequestHeaderId = @IPServiceRequestHeaderId 

WHILE(@DrugCount> 0)
BEGIN

INSERT INTO [dbo].[tbl_IPServiceRequestDetails]
           ([PatientId]
           ,[RegistrationNumber]
           ,[AdmissionId]
           ,[AdmissionNumber]
           ,[IPServiceRequestHeaderId]
           ,[OrderNumber]
           ,[ItemName]
           ,[BatchNumber]
           ,[BedId]
           ,[IssuedQuantity]
           ,[PrescribedDoctorId]
           ,[CreatedById]
           ,[UpdatedById]
           ,[HospitalId]
           ,[Price]
           ,[ProductId]
           ,[RequestedQuantity]
           ,[DrugRequestType]
			,Dosage
           ,[Days]
           ,[ExpiryDate]
           ,[PendingQuantity]
           ,[IPPatientMedicationId]
		   ,ServiceRequestDate
		   ,CreatedTime)


SELECT TOP 1 A.PatientId,
  PT.RegistrationNumber,
  A.AdmissionId,
  A.AdmissionNumber,
  PM.IPServiceRequestHeaderId,
  IPH.OrderNUmber,
  PR.ProductName,
  WSD.BatchNumber,
  A.CurrentBedId,
  @DrugApprovedQuantity,
  A.AdmittingConsultantId,
  1,
  1,
  1,
  WSD.MRP,
  PR.ProductId,
  @DrugApprovedQuantity,
  'General',
  3,
  2,
  WSD.ExpiryDate,
  0,
  PM.IPPatientMedicationId,
  IPH.RequestedDateTime,
  IPH.RequestedDateTime
FROM [dbo].[tbl_IPPatientMedication] PM
INNER JOIN tbl_WarehouseStockHeader WSH
ON PM.IPServiceRequestHeaderId = @IPServiceRequestHeaderId
AND PM.ProductId = WSH.ProductId
JOIN tbl_WarehouseStockDetail WSD
ON WSH.WarehouseStockHeaderId = WSD.WarehouseStockHeaderId
INNER JOIN Admission A 
ON A.AdmissionId = PM.AdmissionId
INNER JOIN Patient PT 
ON PT.PatientId = A.PatientId
INNER JOIN tbl_IPServiceRequestHeader IPH
ON IPH.IPServiceRequestHeaderId = PM.IPServiceRequestHeaderId
INNER JOIN MST.tbl_Product PR
ON PR.ProductId = PM.ProductId
LEFT JOIN tbl_IPServiceRequestDetails IPD
ON IPD.IPServiceRequestHeaderId = @IPServiceRequestHeaderId 
AND IPD.ProductId = PM.ProductId
WHERE IPD.ProductId IS NULL

ORDER BY NEWID()

SET @DRUGCOUNT = @DrugCount - 1;

END

/*************************************************************IP Patient Medication order End**************************************************************/


UPDATE tbl_IPServiceRequestDetails
SET PatientNetAmount = Price
WHERE AdmissionId = @AdmissionId

/*************************************************************IP Patient Billing Start**************************************************************/


INSERT INTO [dbo].[tbl_IPBillInvoiceHeader]
  ([InvoiceNumber]
      ,[PatientId]
      ,[InvoiceAmount]
      ,[InvoiceDate]
      ,[SettledAmount]
      ,[BalanceAmount]
      ,[EntitlementId]
      ,[PatientGrossAmount]
      ,[SponsorGrossAmount]
      ,[PatientLineDiscount]
      ,[SponsorLineDiscount]
      ,[PatientGrossDiscountAmount]
      ,[SponsorGrossDiscountAmount]
      ,[RefDocNumber]
      ,[IsCancelled]
      ,[CancelledDate]
      ,[CancelledByUserId]
      ,[AdmissionId]
      ,[PatientSurchargeAmount]
      ,[SponsorSurchargeAmount]
      ,[PatientTaxAmount]
      ,[SponsorTaxAmount]
      ,[PatientRoundOffAmount]
      ,[SponsorRoundOffAmount]
      ,[ICDId]
      ,[ReferralName]
      ,[PatientTypeId]
      ,[AuthorisedById]
      ,[BillReceiptReasonCode]
      ,[ManualReceiptNumber]
      ,[PatientClassCode]
      ,[PatientNetAmount]
      ,[SponsorNetAmount]
      ,[HospitalId]
      ,[CreatedById]
      ,[UpdatedById]
      ,[CreatedTime]
      ,[LastUpdatedTime]
      ,[IsActive]
      ,[DeleteComments]
      ,[InvoiceIndicator]
      ,[ReferenceInvoiceNumber]
      ,[PatientFNSCAmount]
      ,[SponsorFNSCAmount]
      ,[PatientEmergencyCharges]
      ,[SponsorEmergencyCharges]
      ,[CreditAmount]
      ,[DebitAmount]
      ,[settlementNumber]
      ,[BillTypeId]
      ,[BillCancelReasonId])
SELECT SQ.[Prefix]+'-'+ CAST(NEXT VALUE FOR Seq_IPINV_Number AS VARCHAR )+'-' +SQ.[Suffix] AS [InvoiceNumber]
      ,[PatientId]
      ,0 AS [InvoiceAmount]
      ,GETDATE() AS [InvoiceDate]
      ,0 AS [SettledAmount]
      ,0 AS [BalanceAmount]
      ,NULL AS [EntitlementId]
      ,0 AS [PatientGrossAmount]
      ,0 AS [SponsorGrossAmount]
      ,0 AS [PatientLineDiscount]
      ,0 AS [SponsorLineDiscount]
      ,0 AS [PatientGrossDiscountAmount]
      ,0 AS [SponsorGrossDiscountAmount]
      ,NULL AS [RefDocNumber]
      ,0 AS [IsCancelled]
      ,NULL AS [CancelledDate]
      ,NULL AS [CancelledByUserId]
      ,[AdmissionId]
      ,0 AS [PatientSurchargeAmount]
      ,0 AS [SponsorSurchargeAmount]
      ,0 AS [PatientTaxAmount]
      ,0 AS [SponsorTaxAmount]
      ,0 AS [PatientRoundOffAmount]
      ,0 AS [SponsorRoundOffAmount]
      ,NULL AS [ICDId]
      ,NULL AS [ReferralName]
      ,(SELECT PatientTypeId FROM mst.patienttype WHERE PatientTypeCode = 'IP' AND ISACTIVE =1) AS [PatientTypeId]
      ,NULL AS [AuthorisedById]
      ,NULL AS [BillReceiptReasonCode]
      ,NULL AS [ManualReceiptNumber]
      ,PC.[PatientClassCode]
      ,0 AS [PatientNetAmount]
      ,0 AS [SponsorNetAmount]
      ,A.[HospitalId]
      ,A.[CreatedById]
      ,A.[UpdatedById]
      ,GETDATE() AS [CreatedTime]
      ,GETDATE() AS [LastUpdatedTime]
      ,A.[IsActive]
      ,NULL AS [DeleteComments]
      ,SQ.SequenceCode AS [InvoiceIndicator]
      ,NULL AS [ReferenceInvoiceNumber]
      ,0 AS [PatientFNSCAmount]
      ,0 AS [SponsorFNSCAmount]
      ,0 AS [PatientEmergencyCharges]
      ,0 AS [SponsorEmergencyCharges]
      ,0 AS [CreditAmount]
      ,0 AS [DebitAmount]
      ,NULL AS [settlementNumber]
      ,BT.[BillTypeId]
      ,NULL AS [BillCancelReasonId]
FROM Admission A 
INNER JOIN [mst].[SequenceGenerator] SQ
ON SQ.SequenceCode = 'IPINV' AND SQ.IsActive = 1
INNER JOIN [mst].[tbl_BillType] BT
ON BT.BillTypeCode = 'HSP'
AND BT.IsActive = 1
INNER JOIN mst.PatientClass PC
ON PC.PatientClassId = A.BillingClassId
WHERE AdmissionId = @AdmissionId


SELECT @IPBillInvoiceHeaderId  = @@IDENTITY


INSERT INTO [dbo].[tbl_IPBillInvoiceDetail]
	([IPBillInvoiceHeaderId]
	,[AdmissionId]
	,[PatientId]
	,[ServiceId]
	,[ServiceAmount]
	,[Unit]
	,[PatientGrossAmount]
	,[SponsorGrossAmount]
	,[PatientDiscountAmount]
	,[SponsorDiscountAmount]
	,[PatientSurchargeAmount]
	,[SponsorSurchargeAmount]
	,[PatientTaxAmount]
	,[SponsorTaxAmount]
	,[PatientNetAmount]
	,[SponsorNetAmount]
	,[PatientGrossDiscountAmount]
	,[SponsorGrossDiscountAmount]
	,[RefDocNumber]
	,[InvoiceIndicator]
	,[HospitalId]
	,[HospitalTariff]
	,[PrescribedDoctorId]
	,[LMPDate]
	,[STAT]
	,[ServicePCAmount]
	,[IsCancelled]
	,[CancelledDate]
	,[CancelledByUserId]
	,[CreatedById]
	,[UpdatedById]
	,[CreatedTime]
	,[LastUpdatedTime]
	,[IsActive]
	,[DeleteComments]
	,[PatientFNSCAmount]
	,[SponsorFNSCAmount]
	,[PatientEmergencyCharges]
	,[SponsorEmergencyCharges]
	,[SettlementNumber]
	,[CreditNoteReasonId]
	,[Remarks]
	,[IPServiceRequestDetailsId]
	,[SplitQuantity]
	,[IsServiceRenderred])
SELECT @IPBillInvoiceHeaderId
	,[AdmissionId]
	,[PatientId]
	,[ServiceId]
	,PRICE AS [ServiceAmount]
	,IssuedQuantity AS [Unit]
	,[PatientGrossAmount]
	,[SponsorGrossAmount]
	,[PatientDiscountAmount]
	,[SponsorDiscountAmount]
	,[PatientSurchargeAmount]
	,[SponsorSurchargeAmount]
	,[PatientTaxAmount]
	,[SponsorTaxAmount]
	,[PatientNetAmount]
	,[SponsorNetAmount]
	,[PatientGrossDiscountAmount]
	,[SponsorGrossDiscountAmount]
	,NULL AS [RefDocNumber]
	,'IPINV' AS [InvoiceIndicator]
	,[HospitalId]
	,[HospitalTariff]
	,[PrescribedDoctorId]
	,[LMPDate]
	,[STAT]
	,0 AS [ServicePCAmount]
	,[IsCancelled]
	,[CancelledDate]
	,[CancelledByUserId]
	,[CreatedById]
	,[UpdatedById]
	,[CreatedTime]
	,[LastUpdatedTime]
	,[IsActive]
	,NULL AS [DeleteComments]
	,[PatientFNSCAmount]
	,[SponsorFNSCAmount]
	,[PatientEmergencyCharges]
	,[SponsorEmergencyCharges]
	,NULL AS [SettlementNumber]
	,NULL AS [CreditNoteReasonId]
	,[Remarks]
	,[IPServiceRequestDetailsId]
	,[SplitQuantity]
	,[IsServiceRenderred]
FROM tbl_IPServiceRequestDetails
WHERE AdmissionId = @AdmissionId 
AND IsActive = 1 AND ISNULL(IsCancelled ,0) = 0 


UPDATE H
SET InvoiceAmount = T.InvoiceAmount, SettledAmount = T.InvoiceAmount
FROM (
	SELECT SUM(d.ServiceAmount) AS InvoiceAmount
	FROM tbl_IPBillInvoiceDetail D 
	WHERE D.IPBillInvoiceHeaderId = @IPBillInvoiceHeaderId) T
INNER JOIN tbl_IPBillInvoiceHeader H
ON H.IPBillInvoiceHeaderId = @IPBillInvoiceHeaderId


/*************************************************************IP Patient Billing End**************************************************************/


/*************************************************************Patient Discharge start**************************************************************/
IF @DischargePatient = 1 
BEGIN

INSERT INTO [dbo].[tbl_PatientDischarge]
([AdmissionId]
      ,[AdmissionNumber]
      ,[DischargeTypeId]
      ,[DischargeReasonId]
      ,[DischargeDateTime]
      ,[TransferLocation]
      ,[DischargeStatusId]
      ,[DisplayOrder]
      ,[BlackListId]
      ,[FinancialClearanceReasonId]
      ,[ExpiredDateTime]
      ,[DischargeTypeRemark]
      ,[IsVisitorPassReturned]
      ,[IsFinancialClearance]
      ,[VisitorPassNotReturnReason]
      ,[IsFeedbackFormFilled]
      ,[ClearedById]
      ,[DischargeCancelReasonId]
      ,[DischargeCancelAuthorizerId]
      ,[CancelRemark]
      ,[CreatedById]
      ,[UpdatedById]
      ,[CreatedTime]
      ,[LastUpdatedTime]
      ,[HospitalId]
      ,[IsActive])
SELECT A.[AdmissionId]
      ,A.[AdmissionNumber]
      ,(SELECT [DischargeTypeId] FROM [mst].[tbl_DischargeType] WHERE DischargeTypeCode = 'NRM') AS [DischargeTypeId]
      ,(SELECT TOP 1 [DischargeReasonId] FROM [mst].[tbl_DischargeReason] WHERE IsActive = 1 ORDER BY NEWID()) AS [DischargeReasonId]
      ,DATEADD(HH, DisplayOrder, @DischargeDateTime ) AS [DischargeDateTime]
      ,NULL AS [TransferLocation]
      ,DS.[DischargeStatusId]
      ,DS.[DisplayOrder]
      ,NULL AS [BlackListId]
      ,(SELECT TOP 1 [FinancialClearanceReasonId] FROM [mst].[tbl_FinancialClearanceReason] WHERE IsActive = 1 ORDER BY NEWID() ) 
      ,NULL AS [ExpiredDateTime]
      ,'Normal Dischrage: Treatment completed' AS [DischargeTypeRemark]
      ,1 AS [IsVisitorPassReturned]
      ,1 AS [IsFinancialClearance]
      ,NULL AS [VisitorPassNotReturnReason]
      ,1 AS [IsFeedbackFormFilled]
      ,(SELECT TOP 1 EmployeeId FROM dbo.tbl_Employee WHERE IsActive =1 ORDER BY NEWID())
      ,NULL AS [DischargeCancelReasonId]
      ,NULL AS [DischargeCancelAuthorizerId]
      ,NULL AS [CancelRemark]
      ,A.[CreatedById]
      ,A.[UpdatedById]
      ,A.[CreatedTime]
      ,A.[LastUpdatedTime]
      ,A.[HospitalId]
      ,A.[IsActive]
FROM Admission A
INNER JOIN mst.tbl_DischargeStatus DS
ON DS.IsActive = 1
WHERE A.AdmissionId = @AdmissionId 
ORDER BY DS.DisplayOrder


INSERT INTO [dbo].[tbl_DischargeSummaryHeader]
	([AdmissionId]
	,[AdmissionNumber]
	,[AuthorizingDoctorId]
	,[IsSummaryFreezed]
	,[DischargeUnFreezeReasonId]
	,[CreatedById]
	,[UpdatedById]
	,[CreatedTime]
	,[LastUpdatedTime]
	,[HospitalId]
	,[IsActive])
SELECT PD.[AdmissionId]
	,PD.[AdmissionNumber]
	,A.AdmittingConsultantId
	,0 AS [IsSummaryFreezed]
	,NULL AS [DischargeUnFreezeReasonId]
	,PD.[CreatedById]
	,PD.[UpdatedById]
	,PD.[CreatedTime]
	,PD.[LastUpdatedTime]
	,PD.[HospitalId]
	,PD.[IsActive]
FROM ADMISSION A INNER JOIN 
(SELECT TOP 1 * FROM [dbo].[tbl_PatientDischarge] WHERE AdmissionId =  @AdmissionId ORDER BY DisplayOrder) PD 
ON PD.AdmissionId = A.AdmissionId

SET @DischargeSummaryHeaderId  = @@IDENTITY

INSERT INTO [dbo].[tbl_DischargeSummaryDetail]
	([DischargeSummaryHeaderId]
	,[DischargeCategoryId]
	,[DischargeSummary]
	,[CategoryViewOrder]
	,[CreatedById]
	,[UpdatedById]
	,[CreatedTime]
	,[LastUpdatedTime]
	,[HospitalId]
	,[IsActive])
SELECT
	 DSH.[DischargeSummaryHeaderId]
	,DC.[DischargeCategoryId]
	,DC.DischargeCategoryDescription + ' Summary' AS [DischargeSummary]
	,DC.[CategoryViewOrder]
	,DSH.[CreatedById]
	,DSH.[UpdatedById]
	,DATEADD(HH,DC.[CategoryViewOrder] , DSH.[CreatedTime])
	,DATEADD(HH,DC.[CategoryViewOrder] , DSH.[CreatedTime])
	,DSH.[HospitalId]
	,DSH.[IsActive]
FROM [dbo].[tbl_DischargeSummaryHeader] DSH
INNER JOIN [mst].[tbl_DischargeCategory] DC
ON DC.IsActive = 1
WHERE DischargeSummaryHeaderId=  @DischargeSummaryHeaderId
ORDER BY DC.[CategoryViewOrder] 


INSERT INTO [dbo].[tbl_DischargePatientChecklist]
	([PatientDischargeId]
	,[DischargeStatusId]
	,[DischargeChecklistId]
	,[IsChecked]
	,[ActionPerformedStatusId]
	,[CreatedById]
	,[UpdatedById]
	,[CreatedTime]
	,[LastUpdatedTime]
	,[HospitalId]
	,[IsActive])
SELECT 
	 PD.[PatientDischargeId]
	,PD.[DischargeStatusId]
	,DC.[DischargeChecklistId]
	,1 AS [IsChecked]
	,PD.[DischargeStatusId] AS [ActionPerformedStatusId]
	,PD.[CreatedById]
	,PD.[UpdatedById]
	,PD.[CreatedTime]
	,PD.[LastUpdatedTime]
	,PD.[HospitalId]
	,PD.[IsActive]
FROM [dbo].[tbl_PatientDischarge] PD
INNER JOIN [mst].[tbl_DischargeChecklist] DC
ON DC.DischargeStatusId = PD.DischargeStatusId
AND DC.IsActive = 1
WHERE @AdmissionId = PD.AdmissionId

UPDATE A
SET DischargeStatusId = DS.[DischargeStatusId]
, DischargeDate = PD.DischargeDateTime
FROM tbl_PatientDischarge PD
INNER JOIN [mst].[tbl_DischargeStatus] DS
ON DS.DischargeStatusCode = 'PHDI'
INNER JOIN Admission A 
ON A.AdmissionId = @AdmissionId
WHERE PD.AdmissionId = @AdmissionId
AND PD.DischargeStatusId = DS.DischargeStatusId

UPDATE mst.BED
SET BedCurrentAdmissionNumber = NULL, BedCurrentGender = NULL, BedCurrentPatientId = NULL , PatientBillingClassId = NULL, BedHoldingPatientId = NULL
WHERE BedCurrentAdmissionNumber = @AdmissionId

END

/*************************************************************Patient Discharge End**************************************************************/



SET @RsCount = @RsCount - 1;
END
SET @StartDate = DATEADD(DD, 1, @StartDate)
END