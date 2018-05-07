
/*Script will perform following tasks for single patient:
1.create appointment
2.bill Appointments
3.Create service orders
4.Drug Entry
5.insert chief summary, complaints, vitals & medications
6.bill service orders


*/

/********************************************inputs********************************************/
DECLARE
@NoOfDays int =1,								----For How many days appointments need to be created
@DailyAppointments1 int = 1 ,					----How many appointments per day
@doctorid bigint = 11,							----For which Doctor you need to create appointments
@patientid int = 374212,						----For which patient you need to create appointments
@date datetime = '2017-12-07 00:00:00.000',		----From which Date you need to create appointments
/********************************************inputs********************************************/

@ConsultationTypeId int, 
@DCount int, @Weekday Varchar(20), @time time, @id int, @OB INT, @AppointmentId int,
@DailyAppointments int, @Upper INT = 5, @Lower INT = -5, @AGE INT, @ChiefComplaintId  int,@OrderId int, @frequencyId INT, @Units INT, @warehouseId INT = 28

,@Interval int


--SELECT @date ='2017-04-13 00:00:00.000'

IF OBJECT_ID('tempdb..#patient') IS NOT NULL
DROP TABLE #patient

SELECT * INTO #patient
FROM [dbo].[Patient]
WHERE PatientId =  @patientid

SELECT @AGE = DATEDIFF(YEAR, DOB, GETDATE())
FROM #patient

IF OBJECT_ID('tempdb..#Complaint') IS NOT NULL
DROP TABLE #Complaint
SELECT DISTINCT CC.ChiefComplaintId, CC.Description INTO #Complaint
FROM ChiefComplaint CC 
WHERE Description IN (
 'Chest Pain'
,'Abdominal Pain'
,'Breathing Difficulty'
)


IF OBJECT_ID('tempdb..#doctor') IS NOT NULL
DROP TABLE #doctor 

SELECT identity(int , 1, 1) as id,D1.doctorid into #doctor 
FROM mst.Doctor D1 INNER JOIN mst.Doctor D2 ON  1 = 0

INSERT into #doctor 
SELECT DOCTORID from mst.Doctor D  where D.DoctorId = @doctorid

SELECT @DCount = COUNT(ID) FROM #doctor

SELECT @OB = OverbookCount FROM mst.DoctorDiary WHERE DoctorId = @doctorid AND IsActive = 1 AND WeekDay = DATENAME(WEEKDAY, @date) 

--SELECT @OB

IF OBJECT_ID('tempdb..#AppointmentInterval') IS NOT NULL
DROP TABLE #AppointmentInterval 
CREATE TABLE #AppointmentInterval (AppointmentTime TIME, DoctorId bigint, Weekday Varchar(20), ConsultationTypeId bigint,OB int, Slot int, HospitalId BIGINT)

declare @weekdayId int = 7

WHILE (@weekdayId > 0)
BEGIN
	SELECT @weekday = WeekDay FROM mst.WeekDay
	WHERE WeekDayId = @weekdayId

	INSERT INTO #AppointmentInterval (AppointmentTime , DoctorId, Weekday, ConsultationTypeId, OB, Slot, HospitalId)
	EXEC AppointmentTimeInterval @doctorid, @WEEKDAY

	SET @weekdayId = @weekdayId -1;
END
IF OBJECT_ID('tempdb..#AppointmentTime') IS NOT NULL
DROP TABLE #AppointmentTime
CREATE TABLE #AppointmentTime (id int identity(1,1), AppointmentTime TIME, DoctorId bigint, Weekday Varchar(20), ConsultationTypeId bigint, Isused int default(0))
insert into  #AppointmentTime 
(AppointmentTime , DoctorId, Weekday, ConsultationTypeId)
select AppointmentTime , DoctorId, Weekday, ConsultationTypeId from #AppointmentInterval


--SELECT * FROM #AppointmentTime


WHILE (@NoOfDays> 0)
BEGIN

--WHILE(@DailyAppointments  < 15)
--BEGIN
--SELECT @DailyAppointments = cast (RAND() * 20 + 1 as int)
--END
  --DECLARE @DailyAppointments int
SET @DailyAppointments = @DailyAppointments1


SET @Weekday = DATENAME(WEEKDAY, @date)

WHILE (@DailyAppointments > 0)
BEGIN


	SELECT TOP 1 @patientid = patientid FROM #Patient
	SELECT TOP 1 @ChiefComplaintId = ChiefComplaintId FROM #Complaint ORDER BY newid()

	SELECT TOP 1 @Time = AppointmentTime , @ConsultationTypeId= at.ConsultationTypeId, @id = id
	FROM #AppointmentTime AT 
	LEFT JOIN DoctorAppointment DA ON AT.DoctorId = DA.DoctorId
		AND DA.AppointmentStartTime = DATEDIFF(dd, 0,@date) + CONVERT(DATETIME, AppointmentTime)
	WHERE AT.DoctorId = @doctorid AND (( (@OB != 0 AND DA.IsOverBooked = 0) OR DA.AppointmentId IS NULL) 
		OR ( @OB = 0 AND DA.IsOverBooked = 0 AND DA.AppointmentId IS NULL))
		AND Isused = 0 and Weekday = @Weekday and Isused = 0
		ORDER BY newid()
			

	
	IF EXISTS (
		SELECT 1
		FROM #AppointmentTime AT 
	LEFT JOIN DoctorAppointment DA ON AT.DoctorId = DA.DoctorId
		AND DA.AppointmentStartTime = DATEDIFF(dd, 0,@date) + CONVERT(DATETIME, AppointmentTime)
	WHERE AT.DoctorId = @doctorid AND (( (@OB != 0 AND DA.IsOverBooked = 0) OR DA.AppointmentId IS NULL) 
		OR ( @OB = 0 AND DA.IsOverBooked = 0 AND DA.AppointmentId IS NULL))
		AND Isused = 0 and Weekday = @Weekday and Isused = 0
			
	) 
		
	
BEGIN

			
		INSERT INTO [dbo].[DoctorAppointment]
		( [AppointmentTypeId]
		,[PatientId]
		,[RegistrationNumber]
		,[RegistrationTypeId]
		,[DoctorId]
		,[ConsultationTypeId]
		,[Reason]
		,[AppointmentStartTime]
		,[AppointmentEndTime]
		,[IsOverBooked]
		,[FollowUpDate]
		,[RecordingDate]
		,[IsVisitClosed]
		,[CancelComment]
		,[SendSMS]
		,[SendEmail]
		,[HospitalId]
		,[CreatedTime]
		,[CreatedById]
		,[LastUpdatedTime]
		,[UpdatedById]
		,[IsActive]
		,[SlotSequenceOrder]
		,[FirstConsultation]
		,[IsFirstConsultation]
		,[IsWalkIn]
		,[ArrivalTime]
		,[ConsultedTime]
		,[IsCreatedByPatient]
		,[IsCancelledByPatient]
		,[IsUpdatedByPatient]
		,[ScreeningTime]
		,[TransferToDoctorId]
		,AppointmentStatusId)
		
		SELECT AT.[AppointmentTypeId]
		,p.PatientId
		,P.[RegistrationNumber]
		,P.[RegistrationTypeId]
		,@doctorid
		,@ConsultationTypeId
		,Null
		,DATEDIFF(dd, 0,@date) + CONVERT(DATETIME,@time) --dateadd(MINUTE, ,@Date)--
		,DATEDIFF(dd, 0,@date) + CONVERT(DATETIME,dateadd(MINUTE,15+cast(rand()*900 as int)%5, @time))
		, 0
		, null
		, DATEDIFF(dd, 0,@date) + CONVERT(DATETIME,@time)
		, null
		, null
		, 0
		, 0
		, 1
		,@Date
		, 1
		,@Date
		, 1
		, 1
		, null
		, null
		, 0
		, 0
		, null
		, DATEDIFF(dd, 0,@date) + CONVERT(DATETIME,@time) 
		, 0
		, 0
		, 0
		, null
		, null
		,APS.AppointmentStatusId
		FROM #Patient P 
		LEFT JOIN mst.AppointmentType AT ON AT.AppointmentTypeId IN (select CAST(RAND() * max(AppointmentTypeId ) + 1 AS INT) from mst.AppointmentType 
		where AT.IsActive = 1)
		INNER JOIN MST.AppointmentStatus APS
		ON APS.AppointmentStatus = 'TENTATIVE'

		WHERE PatientId = @PatientId 


		SELECT @AppointmentId = MAX(AppointmentId) FROM DoctorAppointment
		WHERE PatientId = @patientid
		--delete from #patient where Patientid = @patientid
		IF(@AppointmentId IS NULL)
		BEGIN
		SET @DailyAppointments = @DailyAppointments-1;
		CONTINUE;
		END
				  
		UPDATE DoctorAppointment
		SET IsOverBooked = 1
		WHERE AppointmentStartTime  IN (
		SELECT DA.AppointmentStartTime 
		FROM (
		SELECT AppointmentStartTime, DoctorId, COUNT(1) CNT
		FROM DoctorAppointment 
		WHERE DoctorId = @doctorid AND PatientID = @patientid
		AND CONVERT(DATE, AppointmentStartTime) = CONVERT(DATE, @date)
		GROUP BY AppointmentStartTime, DoctorId
		)DA 
		INNER JOIN (
		SELECT AppointmentTime , WEEKDAY, DoctorId, COUNT(1) CNT
		FROM #AppointmentTime 
		GROUP BY AppointmentTime , WEEKDAY, DoctorId
		) AT
		ON CONVERT(TIME, AT.AppointmentTime ) = AT.AppointmentTime
		AND AT.DoctorId = DA.DoctorId AND AT.Weekday = DATENAME(WEEKDAY, DA.AppointmentStartTime)
		AND DA.CNT = AT.CNT
		)

		UPDATE #AppointmentTime
		SET Isused = 1
		WHERE id = @id
		--DoctorId = @doctorid and ConsultationTypeId = @ConsultationTypeId and Isused = 0 and AppointmentTime = @time
				 
		/*
		add complaints 
		*/
				   
		INSERT INTO [dbo].[PatientComplaint]
		([AppointmentId]
			,[ComplaintSince]
			,[ComplaintId]
			,[HOPI]
			,[CreatedById]
			,[UpdatedById]
			,[CreatedTime]
			,[LastUpdatedTime]
			,[IsActive]
			,[HospitalId]
			,[PrimaryDoctorId])

		SELECT @AppointmentId, '2-3 Days' AS [ComplaintSince], 
		cc.ChiefComplaintId, NULL, 1, 1, GETDATE(), GETDATE(), 1, 1, NULL
		FROM DoctorAppointment DA
		CROSS JOIN #Complaint CC
		WHERE CC.ChiefComplaintId = @ChiefComplaintId
		GROUP BY cc.ChiefComplaintId

		INSERT INTO [dbo].[ChiefSummary]
			([AppointmentId]
			,[RecordingDate]
			,[CaseSheet]
			,[ConfidentialHistory]
			,[CanbeShownToOtherDoctors]
			,[IsReferredOrTransferred]
			,[ReferredTo]
			,[TransferredTo]
			,[ReferBack]
			,[VisitClose]
			,[CreatedById]
			,[UpdatedById]
			,[CreatedTime]
			,[LastUpdatedTime]
			,[IsActive]
			,[HospitalId]
			,[PrimaryDoctorId]
			,[IsClinicalPatient]
			,[ClinicalPatientRemarks]
			,[DoctorId]
			,[Conclusion])
		SELECT TOP 1 @AppointmentId, DATEDIFF(dd, 0,@date) + CONVERT(DATETIME,@time), 
		'The Patient is '+CAST(@AGE AS VARCHAR)+' year old. He is suffering from '+Description+' since last 2-3 days.',
		NULL, 0 , 0, NULL, NULL, 0, 0,  1, 1, GETDATE(), GETDATE(), 1, 1, NULL, NULL, NULL, @doctorid, NULL
		FROM #Complaint
		WHERE ChiefComplaintId = @ChiefComplaintId

		INSERT INTO [dbo].[PatientDiagnosis]
		([AppointmentId]
		,[ICDId]
		,[DiagnosisType]
		,[Remarks]
		,[CreatedById]
		,[UpdatedById]
		,[CreatedTime]
		,[LastUpdatedTime]
		,[IsActive]
		,[HospitalId]
		,[PrimaryDoctorId]
		,[DiagnosisId]
		,[ActiveStatus])
		SELECT @AppointmentId, ICD.ICDId, 'Provisional', '', 1, 1, GETDATE(), GETDATE(), 1, 1, @doctorid, NULL, 1
		FROM #Complaint C
		INNER JOIN [mst].[ICDCode] ICD
		ON  (C.Description LIKE 'Chest Pain' AND ICDCODE = 'R07.9')
		OR (C.Description LIKE 'Abdominal Pain' AND ICDCODE = 'R10')
		OR (C.Description LIKE 'Breathing Difficulty' AND ICDCODE = 'R06.8')
		WHERE C.ChiefComplaintId = @ChiefComplaintId
		


					  
		INSERT INTO [dbo].[PatientVitals]
			([AppointmentId]
			,[Height]
			,[Weight]
			,[BMI]
			,[Systolic]
			,[Pulse]
			,[Temperature]
			,[Respiration]
			,[Comments]
			,[CreatedById]
			,[CreatedTime]
			,[UpdatedById]
			,[LastUpdatedTime]
			,[Diastolic]
			,[IsActive]
			,[HospitalId]
			,[PatientId]
			,[RegistrationNumber])
		SELECT top 1 @AppointmentId
			,[Height]
			,[Weight]+ROUND(((@Upper - @Lower -1) * RAND() + @Lower), 0)
			,cast (([Weight]+ROUND(((@Upper - @Lower -1) * RAND() + @Lower), 0))*10000/(Height*Height) as numeric (10,2))
			,[Systolic]
			,CASE WHEN @NoOfDays%4 = 0 THEN 60+cast(rand()*9090 as int)%15  ELSE  50+cast(rand()*9090 as int)%40 END as [Pulse]
			,CASE WHEN @NoOfDays%4 = 0 THEN 98+cast(rand()*90450 as int)%2  ELSE 97.6	+cast(rand()*90450 as int)%6 END as [Temperature]
			,CASE WHEN @NoOfDays%4 = 0 THEN 12+cast(rand()*90450 as int)%5  ELSE 10+cast(rand()*90450 as int)%8 END as [Respiration]
			,NULL
			,1
			,GETDATE()
			,1
			,GETDATE()
			,[Diastolic]
			,1
			,1
			,P.[PatientId]
			,P.[RegistrationNumber]
		FROM PatientVitals PV
		INNER JOIN Patient P
		ON P.PatientId = @patientid
		ORDER BY [PatientVitalId] DESC

		SELECT TOP 1 @frequencyId = FrequencyId, @Units = Units FROM mst.Frequency WHERE IsActive = 1 ORDER BY NEWID()

		INSERT INTO [dbo].[PatientMedication]
			   ([AppointmentId]
			   ,[MedicationType]
			   ,[TradeName]
			   ,[GenericName]
			   ,[Days]
			   ,[UsageDirection]
			   ,[CreatedById]
			   ,[UpdatedById]
			   ,[DrugIntakeTypeId]
			   ,[FrequencyId]
			   ,[HospitalId]
			   ,[DrugId]
			   ,[PrimaryDoctorId]
			   ,[IsSPR]
			   ,[IsPrescribed]
			   ,[Dosage])
		SELECT TOP 2 
			@AppointmentId, 
			'Trade',
			Prod.DisplayName,
			G.GenericName,
			1+CAST ( RAND()*9880 AS INT) %4,
			Prod.UsageDirection,
			1,
			1,
			Prod.DrugInTakeTypeId,
			@frequencyId,
			1,
			Prod.ProductId,
			@doctorid,
			0,
			0,
			1+CAST ( RAND()*9880 AS INT) %6
		FROM mst.tbl_Product Prod
		LEFT JOIN mst.tbl_Generic G
		ON Prod.GenericId = G.GenericId
		AND Prod.IsActive = 1
		AND G.IsActive = 1
		WHERE Prod.DisplayName IN ('DENSICAL CAP.','SINAREST TAB.1X10''S')
		ORDER BY NEWID()

		UPDATE PatientMedication
		SET Quantity = CEILING(@Units*Dosage*Days)
		WHERE AppointmentId = @AppointmentId

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
			,@AppointmentId
			,@date
			,1,1,@DATE, @date,1,NULL, 1, NULL, NULL, NULL, NULL, NULL, @doctorid, 0


		select @OrderId = @@IDENTITY


		INSERT INTO [dbo].[tbl_OrderProductMap]
			   ([OrderId]
			   ,[ProductId]
			   ,[IsBilled]
			   ,[HospitalId]
			   ,[IssuedQuantity]
			   ,[PendingQuantity]
			   ,[CreatedById]
			   ,[UpdatedById]
			   ,[WarehouseId]
			   ,[RequestedQuantity]
			   ,[FrequencyId]
			   ,[Days]
			   ,[IsCancelled]
			   ,[PrevIssuedQuantity]
			   ,[AppointmentId]
			   ,[DrugInTakeTypeId]
			   ,[PatientMedicationId]
			   ,[UsageDirection]
			   ,[Dosage])
		SELECT DISTINCT @OrderId,
				PM.DrugId,
				0,
				1,
				PM.Quantity,
				0,
				1,
				1,
				WSH.WarehouseId,
				PM.Quantity,
				PM.FrequencyId,
				PM.Days,
				0,
				0,
				@AppointmentId,
				PM.DrugIntakeTypeId,
				PM.PatientMedicationId,
				PM.UsageDirection,
				PM.Dosage
		FROM PatientMedication PM
		JOIN tbl_WarehouseStockHeader WSH
			ON PM.DrugId = WSH.ProductId
			AND WSH.WarehouseId = @warehouseId
		JOIN tbl_WarehouseStockDetail WSD
			ON WSH.WarehouseStockHeaderId = WSD.WarehouseStockHeaderId
		WHERE AppointmentId = @AppointmentId

		UPDATE OPM
		SET BatchNumber = (SELECT TOP 1 WSD.BatchNumber 
							FROM PatientMedication PM
							JOIN tbl_WarehouseStockHeader WSH
								ON PM.DrugId = WSH.ProductId
								AND WSH.WarehouseId = @warehouseId
							JOIN tbl_WarehouseStockDetail WSD
								ON WSH.WarehouseStockHeaderId = WSD.WarehouseStockHeaderId
							WHERE AppointmentId = @AppointmentId
							AND OPM.ProductId = WSH.ProductId
							ORDER BY NEWID())
		FROM tbl_OrderProductMap OPM
		WHERE OrderId = @OrderId


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
			,@AppointmentId
			,@date
			,1,1,@DATE, @date,1,NULL, 1, NULL, NULL, NULL, NULL, NULL, @doctorid, 0


		select @OrderId = @@IDENTITY

					  
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
		select top 2 ServiceId,SectionId 
		from mst.Service  
		WHERE ServiceName IN ('X Ray Chest PA','CT Scan Abdomen','X Ray Abdomen')
		order by newid()							
		union 
		select top 3 ServiceId ,SectionId 
		from mst.Service  
		WHERE ServiceName IN ('Ammonia','Serum Sodium', 'Lipid Profile') 
		order by newid() 
		)t10
		order by newid()
		)S 
		INNER JOIN mst.Section SEC
		ON SEC.SectionId = S.SectionId

		
	END
		SET @DailyAppointments = @DailyAppointments-1;

END
SET @NoOfDays = @NoOfDays-1;

UPDATE #AppointmentTime
SET Isused = 0
WHERE DoctorId = @doctorid  AND Weekday = @Weekday


SET @date = dateadd(dd, 1+CAST(RAND()*90900 AS INT)%3, @Date);

END
	 

