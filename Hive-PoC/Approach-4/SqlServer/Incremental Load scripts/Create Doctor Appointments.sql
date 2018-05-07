-----------------------------Parameters -----------------------------
Declare
@date datetime = '2017-12-07 00:00:00.000', --Start date for creating appointments
@NoOfDays int = 1, ---  for how many days appointments need to be created
@DailyAppointments1 int = 1, --how many daily appointments need to be created
@NoOfPatients INT = 2	--Number of patients to whom daily appointments need to be created

-----------------------------Parameters -----------------------------


DECLARE @patientid int, 
@ConsultationTypeId int, 
@DCount int, 
@Weekday Varchar(20), 
@time time, 
@id int, 
@OB INT,
@DailyAppointments int,
@weekdayId int = 7, 
@doctorid bigint = 149;

IF OBJECT_ID('tempdb..#patient') IS NOT NULL
DROP TABLE #patient

SELECT top (@NoOfPatients) * into #patient
FROM [dbo].[Patient]
WHERE IsActive = 1
ORDER BY NEWID()

IF OBJECT_ID('tempdb..#doctor') IS NOT NULL
DROP TABLE #doctor 

SELECT IDENTITY(int , 1, 1) as id,D1.doctorid 
into #doctor 
FROM mst.Doctor D1 
INNER JOIN mst.Doctor D2 ON  1 = 0

INSERT INTO #doctor 
SELECT DOCTORID 
FROM mst.Doctor D  
WHERE D.DoctorId in( 10,11,15,39)

SELECT @DCount = COUNT(ID) 
FROM #doctor

SELECT @OB = OverbookCount 
FROM mst.DoctorDiary 
WHERE DoctorId = @doctorid AND IsActive = 1 
	AND WeekDay = DATENAME(WEEKDAY, @date) 



IF OBJECT_ID('tempdb..#AppointmentInterval') IS NOT NULL
DROP TABLE #AppointmentInterval 

CREATE TABLE #AppointmentInterval (AppointmentTime TIME, DoctorId bigint, Weekday Varchar(20), ConsultationTypeId bigint,OB int, Slot int, HospitalId BIGINT)


SELECT TOP 1 @doctorid = DoctorId
FROM #doctor
ORDER BY DoctorId


WHILE(@DCount > 0)
BEGIN


SET @weekdayId = 7 ;

	WHILE (@weekdayId > 0)
	BEGIN
		SELECT @weekday = WeekDay 
		FROM mst.WeekDay
		WHERE WeekDayId = @weekdayId

		INSERT INTO #AppointmentInterval (AppointmentTime , DoctorId, Weekday, ConsultationTypeId, OB, Slot, HospitalId)
		EXEC AppointmentTimeInterval @doctorid, @WEEKDAY

		SET @weekdayId = @weekdayId -1;
	END

SET @DCount = @DCount -1;

SELECT TOP 1 @doctorid = DoctorId
FROM #doctor
WHERE @doctorid < DoctorId 
ORDER BY DoctorId

END
IF OBJECT_ID('tempdb..#AppointmentTime') IS NOT NULL
DROP TABLE #AppointmentTime
CREATE TABLE #AppointmentTime (id int identity(1,1), AppointmentTime TIME, DoctorId bigint, Weekday Varchar(20), ConsultationTypeId bigint, Isused int default(0))
INSERT INTO  #AppointmentTime 
(AppointmentTime , DoctorId, Weekday, ConsultationTypeId)
SELECT AppointmentTime , DoctorId, Weekday, ConsultationTypeId FROM #AppointmentInterval


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


		SELECT TOP 1 @patientid = patientid 
		FROM #Patient
		ORDER BY NEWID()

		--SELECT TOP 1 @doctorid = DoctorId
		--FROM #doctor
		--ORDER BY NEWID()


		SELECT TOP 1 @Time = AppointmentTime , @ConsultationTypeId= at.ConsultationTypeId, @id = id, @doctorid = AT.DoctorId  
		FROM #AppointmentTime AT 
		LEFT JOIN DoctorAppointment DA ON AT.DoctorId = DA.DoctorId
			AND DA.AppointmentStartTime = DATEDIFF(dd, 0,@date) + CONVERT(DATETIME, AppointmentTime)
		WHERE --AT.DoctorId = @doctorid AND 
			(( (@OB != 0 AND DA.IsOverBooked = 0) OR DA.AppointmentId IS NULL) 
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
			 ([AppointmentTypeId]
		      ,[PatientId]
		      ,[RegistrationNumber]
		      ,[RegistrationTypeId]
		      ,[DoctorId]
		      ,[ConsultationTypeId]
		      ,[Reason]
		      ,[AppointmentStartTime]
		      ,[AppointmentEndTime]
		      ,AppointmentStatusId
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
		      ,[TransferToDoctorId])
		
			SELECT AT.[AppointmentTypeId]
				  ,p.PatientId
				  ,P.[RegistrationNumber]
				  ,P.[RegistrationTypeId]
				  ,@doctorid
				  ,@ConsultationTypeId
				  ,Null
				  ,DATEDIFF(dd, 0,@date) + CONVERT(DATETIME,@time) --dateadd(MINUTE, ,@Date)--
				  , null
				  ,AppointmentStatusId
				  , 0
				  , null
				  , null
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
				  , null
				  , 0
				  , 0
				  , 0
				  , null
				  , null
				  FROM #Patient P 
				  LEFT JOIN mst.AppointmentType AT ON AT.AppointmentTypeId IN (select CAST(RAND() * max(AppointmentTypeId ) + 1 AS INT)from mst.AppointmentType 
				  where AT.IsActive = 1) 
				  INNER JOIN MST.AppointmentStatus AST
				  ON AST.AppointmentStatus = 'Tentative'

			  
				  WHERE PatientId = @PatientId 

				  --delete from #patient where Patientid = @patientid
			  
				  
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
				 
			  END
			  SET @DailyAppointments = @DailyAppointments-1;

	END
SET @NoOfDays = @NoOfDays-1;

UPDATE #AppointmentTime
SET Isused = 0
WHERE DoctorId = @doctorid  AND Weekday = @Weekday

SET @date = dateadd(dd, 1, @Date);

END
	 