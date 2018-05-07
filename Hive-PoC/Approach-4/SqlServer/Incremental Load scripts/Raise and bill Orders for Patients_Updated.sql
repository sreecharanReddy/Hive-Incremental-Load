

DECLARE 
/********************************Parameters**********************************************/
@StartDate DATETIME = '2017-12-06 00:00:00.000',
@EndDate DATETIME = '2017-12-07 00:00:00.000', 
@PaymentType VARCHAR(20) = 'SPONSOR', ---'SELF PAYING'/'SPONSOR'
@SvcCount INT = 5, --per day per patient service count
@PTCount INT = 1, --per day patient count
/***************************************************************************************/

@ServiceId BIGINT, 
@sectionId BIGINT, 
@DepartmentId BIGINT, 
@InvoiceId BIGINT,
@PatientId BIGINT, 
@OrderId BIGINT, 
@ServiceCount INT, 
@InvNumber VARCHAR(20),
@ReceiptNumber VARCHAR(20),
@ServiceAmount NUMERIC(18,6) = 0.0,
@PatientTypeId BIGINT,
@PatientClassCode Varchar(10) = 'PAT',
@Count INT,
@PatientClassId BIGINT

SELECT @PatientClassId  = PatientClassId
FROM mst.PatientClass
WHERE PatientClassCode = @PatientClassCode




WHILE (@StartDate <= @EndDate)
BEGIN
	SET @Count = @PTCount

	WHILE(@Count > 0)
BEGIN
IF (@PaymentType = 'SELF PAYING')
	SET @PatientId = (SELECT TOP 1 PatientId 
					  FROM Patient P
					  INNER JOIN mst.PaymentType PT
						ON P.PaymentTypeId = PT.PaymentTypeId
						AND P.IsActive = 1
						AND PT.IsActive = 1
					  WHERE PT.PaymentType = @PaymentType
					  ORDER BY NEWID())
IF (@PaymentType = 'SPONSOR')
	SET @PatientId = (SELECT TOP 1 P.PatientId 
					  FROM Patient P
					  INNER JOIN mst.PaymentType PT
						ON P.PaymentTypeId = PT.PaymentTypeId
						AND P.IsActive = 1
						AND PT.IsActive = 1
					  INNER JOIN PatientSponsor PS 
						ON PS.PatientId = P.PatientId AND PS.IsActive = 1
					  WHERE PT.PaymentType = @PaymentType
					  ORDER BY NEWID())

	INSERT INTO [Order]
			   (OrderNumber,
			   PatientId
			   ,OrderDate
			   ,CreatedById
			   ,UpdatedById
			   ,CreatedTime
			   ,LastUpdatedTime
			   ,IsActive
			   ,HospitalId)
		 
		 SELECT NEXT VALUE FOR [dbo].[OrderNumber],
				@PatientId,
				@StartDate,
				1,
				1,
				@StartDate,
				@StartDate,
				1,
				1

	--SET @OrderId = (SELECT OrderId FROM [Order] WHERE PatientId = @PatientId AND OrderDate = @StartDate)

	SELECT @OrderId = @@IDENTITY

	SET @ServiceCount = @SvcCount 

	WHILE(@ServiceCount > 0)
	BEGIN

		SELECT @ServiceId = A.ServiceId, @sectionId = A.SectionId, @DepartmentId = A.DepartmentId
		FROM ( SELECT TOP 1 ServiceId, ST.SectionId, D.DepartmentId
				FROM mst.Service S
				JOIN mst.Section ST
					ON S.SectionId = ST.SectionId 
					AND S.IsActive = 1
					AND ST.IsActive = 1
				JOIN mst.Department D
					ON D.DepartmentId = St.DepartmentId
					AND D.IsActive = 1
					AND D.DepartmentCode IN ('LAB', 'RAD')
					WHERE S.ServiceId IN (
					SELECT ServiceId FROM Tariff
					WHERE IsActive = 1
					AND ServiceTariffPatClassId = @PatientClassId  
					)ORDER BY NEWID())A

		INSERT INTO OrderServiceMap
				   (OrderId
				   ,ServiceId
				   ,CreatedById
				   ,UpdatedById
				   ,CreatedTime
				   ,LastUpdatedTime
				   ,IsActive
				   ,IsBilled
				   ,DepartmentId
				   ,SectionId
				   ,HospitalId
				   ,Quantity)
			
			SELECT @OrderId
				   ,@ServiceId
				   ,1
				   ,1
				   ,@StartDate
				   ,@StartDate
				   ,1
				   ,1
				   ,@DepartmentId
				   ,@sectionId
				   ,1
				   ,1

		SET @ServiceCount = @ServiceCount - 1

	END

	SELECT @InvNumber =  Prefix + '-' +  CAST(NEXT VALUE FOR [dbo].[Seq_INV_Number] AS VARCHAR) + '-' + Suffix 
	FROM mst.SequenceGenerator 
	WHERE Prefix = 'OAD'
	
	
	/*
	
	New insert statement is needed for Sponsor as sposnsor tariff is in different table.([dbo].[ServiceTariffDefinition])
	
	*/
	IF (@PaymentType = 'SELF PAYING')
	BEGIN
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
			   ,IsCancelled
			   )
		
		SELECT @InvNumber,--(SELECT Prefix + '-' +  CAST(NEXT VALUE FOR [dbo].[Seq_INV_Number] AS VARCHAR) + '-' + Suffix FROM mst.SequenceGenerator where PaymentType = 'Invoice'),
				@PatientId,
				0,
				@StartDate,
				0,
				0,
				0,
				1,
				1,
				@StartDate,
				@StartDate,
				1,
				1,
				@OrderId,
				@PatientClassId,
				@PatientClassCode,
				0


	SET @InvoiceId = (SELECT InvoiceId FROM Bill_InvoiceHeader WHERE OrderId = @OrderId)

		INSERT INTO Bill_InvoiceDetail
			   (InvoiceId
			   ,PatientId
			   ,ServiceId
			   ,ServiceAmount
			   ,Unit
			   ,PatientGrossAmount
			   ,InvoiceIndicator
			   ,CreatedById
			   ,UpdatedById
			   ,CreatedTime
			   ,LastUpdatedTime
			   ,IsActive
			   ,HospitalId
			   ,OrderId
			   ,IsCancelled)
		
		SELECT	@InvoiceId,
				@PatientId,
				OSM.ServiceId, 
				T.ServiceTariff, 
				1, 
				T.ServiceTariff, 
				'INV', 
				1, 
				1, 
				@StartDate,
				@StartDate,
				1, 
				1,
				OSM.OrderId
				,0
		FROM OrderServiceMap OSM
		JOIN Bill_InvoiceHeader BIH
			ON BIH.OrderId = OSM.OrderId
		JOIN Tariff T
			ON OSM.ServiceId = T.ServiceId
			AND T.ServiceTariffPatClassId = @PatientClassId  
		WHERE OSM.OrderId = @OrderId
	
	SELECT @ServiceAmount = SUM(ServiceAmount) 
	FROM Bill_InvoiceDetail 
	WHERE InvoiceId = @InvoiceId

	UPDATE BIH
	SET BIH.InvoiceAmount = @ServiceAmount,
	BIH.SettledAmount = @ServiceAmount,
	BIH.PatientGrossAmount = @ServiceAmount,
	BIH.PatientNetAmount = @ServiceAmount
	FROM Bill_InvoiceHeader BIH
	JOIN Bill_InvoiceDetail BID
		ON BIH.InvoiceId = BID.InvoiceId
	WHERE BID.InvoiceId = @InvoiceId
	
	END

	ELSE 
	BEGIN
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
			   ,EntitlementId
			   ,IsCancelled
			   )
		
		SELECT @InvNumber,--(SELECT Prefix + '-' +  CAST(NEXT VALUE FOR [dbo].[Seq_INV_Number] AS VARCHAR) + '-' + Suffix FROM mst.SequenceGenerator where PaymentType = 'Invoice'),
				@PatientId,
				0,
				@StartDate,
				0,
				0,
				0,
				1,
				1,
				@StartDate,
				@StartDate,
				1,
				1,
				@OrderId,
				@PatientClassId,
				@PatientClassCode,
				PS.EntitlementId,
				0
				FROM PatientSponsor PS
				WHERE PS.PatientId = @PatientId
				AND PS.IsActive = 1


	SET @InvoiceId = (SELECT InvoiceId FROM Bill_InvoiceHeader WHERE OrderId = @OrderId)

		INSERT INTO Bill_InvoiceDetail
			(InvoiceId
			,PatientId
			,ServiceId
			,ServiceAmount
			,Unit
			,SponsorGrossAmount
			,InvoiceIndicator
			,CreatedById
			,UpdatedById
			,CreatedTime
			,LastUpdatedTime
			,IsActive
			,HospitalId
			,OrderId
			,IsCancelled)
		SELECT	@InvoiceId,
				@PatientId,
				OSM.ServiceId, 
				CASE WHEN STD.ServiceTariff IS NULL THEN T.ServiceTariff ELSE STD.ServiceTariff END, 
				1, 
				CASE WHEN STD.ServiceTariff IS NULL THEN T.ServiceTariff ELSE STD.ServiceTariff END, 
				'INV', 
				1, 
				1, 
				@StartDate,
				@StartDate,
				1, 
				1,
				OSM.OrderId
				,0
		FROM OrderServiceMap OSM
		INNER JOIN Bill_InvoiceHeader BIH
			ON BIH.OrderId = OSM.OrderId
		INNER JOIN dbo.Entitlement ENT
			ON ENT.EntitlementId = BIH.EntitlementId
			AND ENT.IsActive = 1
		LEFT JOIN ServiceTariffDefinition STD
			ON OSM.ServiceId = STD.ServiceId
			AND STD.InstituteId = ENT.InstituteId
			AND STD.PatientClassId = @PatientClassId  
			AND STD.IsActive = 1
		LEFT JOIN Tariff T
			ON OSM.ServiceId = T.ServiceId
			AND  T.ServiceTariffPatClassId = @PatientClassId  
			AND T.IsActive = 1
		WHERE OSM.OrderId = @OrderId

	SELECT @ServiceAmount = SUM(ServiceAmount) 
	FROM Bill_InvoiceDetail 
	WHERE InvoiceId = @InvoiceId

	UPDATE BIH
	SET BIH.InvoiceAmount = @ServiceAmount,
	BIH.SettledAmount = @ServiceAmount,
	BIH.SponsorGrossAmount = @ServiceAmount,
	BIH.SponsorNetAmount = @ServiceAmount
	FROM Bill_InvoiceHeader BIH
	JOIN Bill_InvoiceDetail BID
		ON BIH.InvoiceId = BID.InvoiceId
	WHERE BID.InvoiceId = @InvoiceId


	END 



	SELECT @ReceiptNumber = Prefix + '-' +  CAST(NEXT VALUE FOR [dbo].[Seq_OPReceipt_Number] AS VARCHAR) + '-' + Suffix 
	FROM mst.SequenceGenerator 
	WHERE Prefix = 'OAD'
	--PaymentType = 'Op-On A/c Deposit'

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
           ,InvoiceId
		   ,IsCancelled)

	SELECT @ReceiptNumber,
			@PatientId,
			InvoiceAmount,
			InvoiceDate,
			'INV',
			1,
			1,
			@StartDate,
			@StartDate,
			1,
			1,
			OrderId,
			InvoiceId
			,0
	FROM Bill_InvoiceHeader
	WHERE InvoiceId = @InvoiceId


	INSERT INTO [dbo].[Bill_ReceiptInvoiceMap]  --Bill_ReciptInvoiceMap
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
			@StartDate,
			@StartDate,
			1,
			1,
			InvoiceId
	FROM Bill_Receipt R
	WHERE InvoiceId = @InvoiceId


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
	SELECT  ReceiptId,
			ReceiptAmount,
			1,
			1,
			@StartDate,
			@StartDate,
			1,
			1,
			2,
			ReceiptAmount
	FROM Bill_Receipt R 
	WHERE InvoiceId = @InvoiceId
				
	SET @Count = @Count - 1

END
	SET @StartDate = DATEADD(DAY,1,@StartDate)
		
END