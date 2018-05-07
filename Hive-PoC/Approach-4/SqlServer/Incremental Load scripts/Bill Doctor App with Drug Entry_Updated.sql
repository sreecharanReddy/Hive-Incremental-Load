-------------------------------------parameters-------------------------------------
DECLARE @STARTDATE DATE = '2017-12-07 00:00:00.000'			--from date
, @ENDDATE DATE = '2017-12-08 00:00:00.000'					--to date
, @patientId int = 22
-------------------------------------parameters-------------------------------------



IF OBJECT_ID('tempdb..#TmpAppointments') IS NOT NULL
DROP TABLE #TmpAppointments

SELECT AppointmentId 
INTO #TmpAppointments
FROM DoctorAppointment DA
INNER JOIN mst.AppointmentStatus APS
ON APS.AppointmentStatusId = DA.AppointmentStatusId
WHERE APS.AppointmentStatus= 'Tentative'
AND AppointmentStartTime > = @STARTDATE AND AppointmentStartTime  < = @ENDDATE
AND PatientId = @patientId
ORDER BY NEWID()


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
		,EntitlementId
		,IsProduct
		)
		
SELECT  Prefix + '-' +  CAST(NEXT VALUE FOR [dbo].[Seq_OPPharmacy_Invoice_Number] AS VARCHAR) + '-' + Suffix ,
		DA.PatientId,
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
		PS.EntitlementId,
		1

	FROM mst.SequenceGenerator SG
	INNER JOIN DoctorAppointment DA
	ON DA.IsActive = 1
	INNER JOIN #TmpAppointments T
	ON SG.IsActive = 1
	INNER JOIN [Order] O
	ON O.AppointmentId = T.AppointmentId	
	AND DA.AppointmentId = T.AppointmentId
	LEFT JOIN PatientSponsor PS
	ON PS.PatientId = DA.PatientId
	AND PS.IsActive = 1
	--WHERE PaymentType = 'OP Pharmacy Invoice'

------------------INVOICE DETAILS

INSERT INTO Bill_InvoiceDetail
		(InvoiceId
		,PatientId
		,ServiceAmount
		,Unit
		,InvoiceIndicator
		,CreatedById
		,UpdatedById
		,HospitalId
		,OrderId
		,[ProductId]
        ,[ProductCostPrice]
        ,[ProductSellingPrice]
        ,[ProductMRP]
        ,[ProductSoldPrice]
        ,[BatchNumber]
        ,[PrevReturnQty]
        ,[IsCancelled]
        ,[ItemName]
		,PatientNetAmount)
		
SELECT	BIH.InvoiceId,
		DA.PatientId, 
		0,
		OPM.IssuedQuantity,
		'PHRINV', 
		1, 
		1, 
		1,
		OPM.OrderId,
		OPM.ProductId,
		WSD.CostPrice,
		WSD.SellingPrice,
		WSD.MRP,
		WSD.SellingPrice,
		OPM.BatchNumber,
		0,
		0,
		Prod.DisplayName,
		OPM.IssuedQuantity * WSD.SellingPrice

FROM tbl_OrderProductMap OPM
INNER JOIN Bill_InvoiceHeader BIH
ON BIH.OrderId = OPM.OrderId
JOIN tbl_WarehouseStockHeader WSH
ON WSH.WarehouseId = OPM.WarehouseId
AND WSH.ProductId = OPm.ProductId
JOIN tbl_WarehouseStockDetail WSD
ON WSD.WarehouseStockHeaderId = WSH.WarehouseStockHeaderId
AND WSD.BatchNumber = OPM.BatchNumber
JOIN mst.tbl_Product Prod
ON WSH.ProductId = Prod.ProductId
INNER JOIN DoctorAppointment DA
ON DA.IsActive = 1
INNER JOIN #TmpAppointments TEMP
ON TEMP.AppointmentId = DA.AppointmentId	
INNER JOIN [Order] O
ON O.AppointmentId = DA.AppointmentId	
AND O.OrderId = OPM.OrderId
INNER JOIN Patient P 
ON P.PatientId = DA.PatientId



UPDATE BIH
SET BIH.InvoiceAmount =  BID.PatientNetAmount ,
BIH.SettledAmount = BID.PatientNetAmount ,
BIH.PatientGrossAmount =  BID.PatientNetAmount,
BIH.PatientNetAmount =  BID.PatientNetAmount,
BIH.SponsorGrossAmount = BID.SponsorGrossAmount,
BIH.SponsorNetAmount= BID.SponsorNetAmount
FROM Bill_InvoiceHeader BIH
JOIN (	SELECT InvoiceId, SUM(ServiceAmount) AS ServiceAmount, SUM(PatientNetAmount) AS PatientNetAmount, SUM(PatientGrossAmount) AS PatientGrossAmount, 
		SUM(SponsorGrossAmount) AS SponsorGrossAmount, SUM(SponsorNetAmount) AS SponsorNetAmount
		FROM Bill_InvoiceDetail 
		GROUP BY InvoiceId) AS BID
ON BIH.InvoiceId = BID.InvoiceId
INNER JOIN DoctorAppointment DA
ON DA.IsActive =1 
INNER JOIN #TmpAppointments T
ON T.AppointmentId = DA.AppointmentId	
INNER JOIN [Order] O
ON O.AppointmentId = DA.AppointmentId
AND BIH.OrderId = O.OrderId



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
		DA.PatientId,
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
INNER JOIN DoctorAppointment DA
ON DA.IsActive = 1
INNER JOIN #TmpAppointments TEMP
ON TEMP.AppointmentId = DA.AppointmentId	
INNER JOIN [Order] O
ON O.AppointmentId = DA.AppointmentId	
AND BIH.OrderId = O.OrderId
LEFT JOIN PatientSponsor PS 
ON PS.PatientId = DA.PatientId 
AND PS.IsActive = 1
--WHERE PaymentType = 'OP Receipt' 
AND PS.PatientSponsorId IS NULL
	
	
INSERT INTO [dbo].[Bill_ReceiptInvoiceMap] --Bill_ReciptInvoiceMap
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
INNER JOIN [Order] O
ON O.OrderId = R.OrderId
INNER JOIN Bill_InvoiceHeader BIH
ON BIH.OrderId= O.OrderId
INNER JOIN DoctorAppointment DA
ON DA.IsActive = 1
INNER JOIN #TmpAppointments TEMP
ON TEMP.AppointmentId = DA.AppointmentId	
AND O.AppointmentId = DA.AppointmentId	
	
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
INNER JOIN [Order] O
ON O.OrderId = R.OrderId
INNER JOIN Bill_InvoiceHeader BIH
ON BIH.OrderId= O.OrderId
INNER JOIN DoctorAppointment DA
ON DA.IsActive = 1
INNER JOIN #TmpAppointments TEMP
ON TEMP.AppointmentId = DA.AppointmentId	
AND O.AppointmentId = DA.AppointmentId	

UPDATE DoctorAppointment
SET AppointmentStatusId = APS.AppointmentStatusId
FROM DoctorAppointment	 DA
INNER JOIN #TmpAppointments T
ON T.AppointmentId = DA.AppointmentId	
INNER JOIN MST.AppointmentStatus APS
ON APS.AppointmentStatus = 'Confirmed'
