drop table fact_Invoice;
create table fact_Invoice(
InvoiceKey SERIAL PRIMARY KEY,
Patientkey BIGINT,
HospitalKey BIGINT,
DoctorKey BIGINT,
ServiceKey BIGINT,
DiagnosisKey BIGINT,
DateTimeKey INT,
ServiceAmount float,
PatientGrossAmount float,
SponsorGrossAmount float,
PatientDiscountAmount float,
SponsorDiscountAmount float,
PatientSurchargeAmount float,
SponsorSurchargeAmount float,
PatientTaxAmount float,
SponsorTaxAmount float,
PatientNetAmount float,
SponsorNetAmount float,
PatientGrossDiscountAmount float,
SponsorGrossDiscountAmount float,
PatientFNSCAmount float,
SponsorFNSCAmount float,
PatientEmergencyCharges float,
SponsorEmergencyCharges float,
SrcUpdatedDate date,
LastModifiedDate TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'Asia/Kolkata')
);


DROP TABLE if exists dim_Service;
create table dim_Service(
	ServiceKey SERIAL PRIMARY KEY,
	ServiceId bigint NOT NULL,
	ServiceName varchar(250) NOT NULL,
	ServiceApptFlag varchar(1) NOT NULL,
	ServiceGroupId bigint NULL,
	ServiceIssueMode varchar(1) NOT NULL,
	ServiceDisplayInstr varchar(500) NULL,
	ServicePrintInstr varchar(500) NULL,
	ServiceTypeId bigint NULL,
	ServiceApplGender varchar(1) NOT NULL,
	ServiceCaseType varchar(1) NOT NULL,
	ServiceDocRequest varchar(1) NOT NULL,
	ServiceDefaultRender varchar(1) NOT NULL,
	ServiceAbbreviation varchar(10) NULL,
	ServiceLinkTo varchar(1) NOT NULL,
	ServiceLMPFlag varchar(1) NOT NULL,
	ServiceMRDFileReq varchar(1) NOT NULL,
	SrcUpdatedDate date,
	LastModifiedDate TIMESTAMP  NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'Asia/Kolkata')
);

DROP TABLE if exists dim_Hospital;
CREATE TABLE dim_Hospital(
	HospitalKey SERIAL PRIMARY KEY,
	HospitalId bigint NOT NULL,
	HospitalName varchar(100) NOT NULL,
	HospitalAddress varchar(500) NULL,
	HospitalWebsite varchar(100) NULL,
	HospitalPhone1 varchar(30) NULL,
	HospitalPhone2 varchar(30) NULL,
	HospitalEmail varchar(100) NULL,
	HospitalFax varchar(10) NULL,
	HospitalCode varchar(5) NULL,
	SrcUpdatedDate date,
	LastModifiedDate TIMESTAMP  NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'Asia/Kolkata')
);

DROP TABLE if exists dim_Doctor;
CREATE TABLE dim_Doctor(
	DoctorKey SERIAL PRIMARY KEY,
	DoctorId bigint NOT NULL,
	FirstName varchar(50) NOT NULL,
	MiddleName varchar(50) NULL,
	LastName varchar(50) NOT NULL,
	QualificationId bigint NULL,
	DepartmentId bigint NOT NULL,
	GenderId bigint NULL,
	NationalityId bigint NOT NULL,
	Experience varchar(20) NULL,
	JoiningDate date NOT NULL,
	DesignationId bigint NOT NULL,
	ServiceNumber varchar(20) NULL,
	Address varchar(250) NOT NULL,
	CityId bigint NULL,
	PinCode varchar(10) NULL,
	ResidenceTelphone varchar(20) NULL,
	OfficeTelphone varchar(20) NULL,
	MobileNumber varchar(20) NULL,
	EmailId varchar(50) NULL,
	DisplayName varchar(150) NOT NULL,
	Photo varchar(100) NULL,
	DigitalSignature varchar(200) NULL,
	UserId bigint NULL,
	IsAnaesthetist bit NULL,
	IsSurgeon bit NULL,
	StateId bigint NULL,
	CountryId bigint NULL,
	EmployeeId bigint NULL,
	LandmarkId bigint NULL,
	IsDiscountAllowed smallint NULL,
	SrcUpdatedDate date,
	LastModifiedDate TIMESTAMP  NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'Asia/Kolkata')
);

DROP TABLE if exists dim_Patient;
CREATE TABLE dim_Patient(
	PatientKey SERIAL PRIMARY KEY,
	PatientId bigint NOT NULL,
	TitleId bigint NULL,
	FirstName varchar(50) NOT NULL,
	MiddleName varchar(50) NULL,
	LastName varchar(50) NULL,
	Gender varchar(50) NULL,
	DOB date NULL,
	ResidenceNumber varchar(20) NULL,
	MobileNumber varchar(20) NULL,
	Email varchar(50) NULL,
	Nationality varchar(50) NULL,
	Image varchar(50) NULL,
	AliasName varchar(50) NULL,
	IsFamilyHead bit NULL,
	MaritalStatus varchar(50) NULL,
	BloodGroup varchar(50) NULL,
	BooldGroupConfirmedBy varchar(50) NULL,
	SrcUpdatedDate date,
	LastModifiedDate TIMESTAMP  NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'Asia/Kolkata')
);

DROP TABLE if exists dim_Diagnosis;
CREATE TABLE dim_Diagnosis(
	DiagnosisKey SERIAL PRIMARY KEY,
	DiagnosisId bigint NOT NULL,
	Diagnosis varchar(150) NULL,
	SrcUpdatedDate date,
	LastModifiedDate TIMESTAMP  NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'Asia/Kolkata')
);


DROP TABLE if exists dim_dateTime;
CREATE TABLE dim_dateTime
(
  date_dim_id              INT NOT NULL,
  date_actual              DATE NOT NULL,
  epoch                    BIGINT NOT NULL,
  day_suffix               VARCHAR(4) NOT NULL,
  day_name                 VARCHAR(9) NOT NULL,
  day_of_week              INT NOT NULL,
  day_of_month             INT NOT NULL,
  day_of_quarter           INT NOT NULL,
  day_of_year              INT NOT NULL,
  week_of_month            INT NOT NULL,
  week_of_year             INT NOT NULL,
  week_of_year_iso         CHAR(10) NOT NULL,
  month_actual             INT NOT NULL,
  month_name               VARCHAR(9) NOT NULL,
  month_name_abbreviated   CHAR(3) NOT NULL,
  quarter_actual           INT NOT NULL,
  quarter_name             VARCHAR(9) NOT NULL,
  year_actual              INT NOT NULL,
  first_day_of_week        DATE NOT NULL,
  last_day_of_week         DATE NOT NULL,
  first_day_of_month       DATE NOT NULL,
  last_day_of_month        DATE NOT NULL,
  first_day_of_quarter     DATE NOT NULL,
  last_day_of_quarter      DATE NOT NULL,
  first_day_of_year        DATE NOT NULL,
  last_day_of_year         DATE NOT NULL,
  mmyyyy                   CHAR(6) NOT NULL,
  mmddyyyy                 CHAR(10) NOT NULL,
  weekend_indr             BOOLEAN NOT NULL,
);

ALTER TABLE public.dim_dateTime ADD CONSTRAINT dim_dateTime_date_dim_id_pk PRIMARY KEY (date_dim_id);

CREATE INDEX dim_dateTime_date_actual_idx
  ON dim_dateTime(date_actual);

COMMIT;

INSERT INTO dim_dateTime
SELECT TO_CHAR(datum,'yyyymmdd')::INT AS date_dim_id,
       datum AS date_actual,
       EXTRACT(epoch FROM datum) AS epoch,
       TO_CHAR(datum,'fmDDth') AS day_suffix,
       TO_CHAR(datum,'Day') AS day_name,
       EXTRACT(isodow FROM datum) AS day_of_week,
       EXTRACT(DAY FROM datum) AS day_of_month,
       datum - DATE_TRUNC('quarter',datum)::DATE +1 AS day_of_quarter,
       EXTRACT(doy FROM datum) AS day_of_year,
       TO_CHAR(datum,'W')::INT AS week_of_month,
       EXTRACT(week FROM datum) AS week_of_year,
       TO_CHAR(datum,'YYYY"-W"IW-') || EXTRACT(isodow FROM datum) AS week_of_year_iso,
       EXTRACT(MONTH FROM datum) AS month_actual,
       TO_CHAR(datum,'Month') AS month_name,
       TO_CHAR(datum,'Mon') AS month_name_abbreviated,
       EXTRACT(quarter FROM datum) AS quarter_actual,
       CASE
         WHEN EXTRACT(quarter FROM datum) = 1 THEN 'First'
         WHEN EXTRACT(quarter FROM datum) = 2 THEN 'Second'
         WHEN EXTRACT(quarter FROM datum) = 3 THEN 'Third'
         WHEN EXTRACT(quarter FROM datum) = 4 THEN 'Fourth'
       END AS quarter_name,
       EXTRACT(isoyear FROM datum) AS year_actual,
       datum +(1 -EXTRACT(isodow FROM datum))::INT AS first_day_of_week,
       datum +(7 -EXTRACT(isodow FROM datum))::INT AS last_day_of_week,
       datum +(1 -EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
       (DATE_TRUNC('MONTH',datum) +INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
       DATE_TRUNC('quarter',datum)::DATE AS first_day_of_quarter,
       (DATE_TRUNC('quarter',datum) +INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
       TO_DATE(EXTRACT(isoyear FROM datum) || '-01-01','YYYY-MM-DD') AS first_day_of_year,
       TO_DATE(EXTRACT(isoyear FROM datum) || '-12-31','YYYY-MM-DD') AS last_day_of_year,
       TO_CHAR(datum,'mmyyyy') AS mmyyyy,
       TO_CHAR(datum,'mmddyyyy') AS mmddyyyy,
       CASE
         WHEN EXTRACT(isodow FROM datum) IN (6,7) THEN TRUE
         ELSE FALSE
       END AS weekend_indr
FROM (SELECT '1970-01-01'::DATE+ SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES (0,29219) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;

COMMIT;



select 
Serviceid
,b.HospitalId
,PrescribedDoctorId
,b.PatientId
,d.DiagnosisId
,SUM(ServiceAmount          ) as ServiceAmount
,SUM(PatientGrossAmount		) as PatientGrossAmount
,SUM(SponsorGrossAmount		) as SponsorGrossAmount
,SUM(PatientDiscountAmount	) as PatientDiscountAmount
,SUM(SponsorDiscountAmount	) as SponsorDiscountAmount
,SUM(PatientSurchargeAmount	) as PatientSurchargeAmount
,SUM(SponsorSurchargeAmount	) as SponsorSurchargeAmount
,SUM(PatientTaxAmount		) as PatientTaxAmount
,SUM(SponsorTaxAmount		) as SponsorTaxAmount
,SUM(PatientNetAmount		) as PatientNetAmount
,SUM(SponsorNetAmount		) as SponsorNetAmount
,SUM(PatientGrossDiscount	) as PatientGrossDiscount
,SUM(SponsorGrossDiscount	) as SponsorGrossDiscount
,SUM(PatientFNSCAmount		) as PatientFNSCAmount
,SUM(SponsorFNSCAmount		) as SponsorFNSCAmount
,SUM(PatientEmergencyCharges) as PatientEmergencyCharges
,SUM(SponsorEmergencyCharges) as SponsorEmergencyCharges
from dbo.Bill_InvoiceDetail b 
inner join dbo.Patient p on b.PatientId = p.PatientId
left join Admission a   on p.PatientId=a.PatientId
left join mst.Diagnosis d on d.DiagnosisId=a.DiagnosisId
group by 
Serviceid
,b.HospitalId
,PrescribedDoctorId
,b.PatientId
,d.DiagnosisId



select * from Bill_InvoiceDetail

select count(*) from mst.Service 
select count(*) from mst.Hospital
select count(*) from mst.Doctor
select count(*) from dbo.Patient
select count(*) from mst.Diagnosis

select count(*) from dim_patient;
select count(*) from dim_diagnosis;
select count(*) from dim_doctor;
select count(*) from dim_service;
select count(*) from dim_hospital;
select count(*) from fact_invoice;

delete from dim_patient;
delete from dim_diagnosis;
delete from dim_doctor;
delete from dim_service;
delete from dim_hospital;
delete from fact_invoice;

select * from mst.Service where LastUpdatedTime > (select run_time from attunity.dbo.control_table)
select * from mst.Hospital where LastUpdatedTime > (select run_time from attunity.dbo.control_table)
select * from mst.Doctor where LastUpdatedTime > (select run_time from attunity.dbo.control_table)
select * from dbo.Patient where LastUpdatedTime > (select run_time from attunity.dbo.control_table)
select * from mst.Diagnosis where LastUpdatedTime > (select run_time from attunity.dbo.control_table)

drop table control_table;

 create table control_table(
 package_name varchar(50),
 run_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP
 );

 insert into control_table(package_name) values('RDBMS to Greenplum data flow Package')
 select * from control_table

 update control_table set run_time= GETDATE() where package_name= 'RDBMS to Greenplum data flow Package';
 select * from control_table
