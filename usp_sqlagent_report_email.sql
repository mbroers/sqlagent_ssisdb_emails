USE [msdb]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[usp_sqlagent_report_email]  @job_id uniqueidentifier, @email_dl nvarchar(255)
AS
BEGIN

-- variables
declare @email_subject nvarchar(255),
@sqlagent_job_fail_count int,
@email_subject_status nvarchar(255),
@bodyHTML_SQLAGENT nvarchar(MAX), 
@bodyHTML nvarchar(MAX),
@job_name nvarchar(255)

--determine job from guid job id
select @job_name = name from msdb.dbo.sysjobs where job_id=@job_id

--generate sql agent status report body
set @bodyHTML_SQLAGENT = N'<H3>' + @job_name + ' on ' + reverse(left((reverse(@@servername)),(charindex('-',reverse(@@servername))-1))) + '</H3><br><H4>SQL Agent Steps</H4>' 
	+ N'<table border = "1">'
	+ N'<th bgcolor = "#AFDACF">Status</th>'
	+ N'<th bgcolor = "#AFDACF">Step</th>'
	+ N'<th bgcolor = "#AFDACF">Step Name</th>'
	+ N'<th bgcolor = "#AFDACF">Start</th>'
	+ N'<th bgcolor = "#AFDACF">Duration</th>'
	+ N'<th bgcolor = "#AFDACF">Type</th>'
	+ N'<th bgcolor = "#AFDACF">Command</th>'
	+ N'<th bgcolor = "#AFDACF">Message</th>'
	+ CAST(( SELECT CASE t2.run_status WHEN 0 THEN '#FF4D4D' WHEN 1 THEN '#8DE28D' ELSE '#FFFF66' END AS [td/@bgcolor]
	, td = CONVERT(VARCHAR(14), (CASE [t2].[run_status] WHEN 0 THEN 'Failed' WHEN 1 THEN 'Succeeded' WHEN 2 THEN 'Retry' WHEN 3 THEN 'Cancelled'  WHEN 4 THEN 'In Progress' END))
	, ''
	, 'center' as [td/@align]
	, td = CONVERT(VARCHAR(255),[t2].[step_id])
	, ''
	, td = CONVERT(VARCHAR(255),[t2].[step_name])
	, ''
	, td = CONVERT(VARCHAR(255),msdb.dbo.agent_datetime(t2.run_date, t2.run_time))
	, ''
	, td = CONVERT(VARCHAR(255),stuff(stuff(replace(str(run_duration,6,0),' ','0'),3,0,':'),6,0,':'))
	, ''
	, td = CONVERT(VARCHAR(255),[t4].[subsystem])
	, ''
	, td = CONVERT(VARCHAR(MAX), (case when t4.subsystem = 'SSIS' then reverse(left(reverse(left(t4.command,charindex('.dtsx',t4.command)+4)),charindex('\',reverse(left(t4.command,charindex('.dtsx',t4.command)+4)))-1)) else t4.command end))
	, ''
	, td = CONVERT(VARCHAR(MAX),[t2].[message]
	)
	FROM    msdb.dbo.sysjobs t1
    JOIN    msdb.dbo.sysjobhistory t2
            ON t1.job_id = t2.job_id 
            --Join to pull most recent job activity per job, not job step
    JOIN    msdb.dbo.sysjobsteps t4
	        on t2.job_id = t4.job_id and t2.step_name = t4.step_name
    JOIN    (
            SELECT  TOP 1
                    t1.job_id
                    ,t1.start_execution_date
                    ,t1.stop_execution_date
            FROM    msdb.dbo.sysjobactivity t1
            --If no job_id detected, return last run job
            WHERE   t1.job_id = COALESCE(@job_id,t1.job_id)
            ORDER 
            BY      last_executed_step_date DESC
            ) t3
            --Filter on the most recent job_id
            ON t1.job_id = t3.job_Id
            --Filter out job steps that do not fall between start_execution_date and stop_execution_date
            AND CONVERT(DATETIME, CONVERT(CHAR(8), t2.run_date, 112) + ' ' 
            + STUFF(STUFF(RIGHT('000000' + CONVERT(VARCHAR(8), t2.run_time), 6), 5, 0, ':'), 3, 0, ':'), 121)  
            BETWEEN t3.start_execution_date AND COALESCE(t3.stop_execution_date,current_timestamp)
	FOR XML PATH('tr'), TYPE) as NVARCHAR(MAX)) + N'</table>'; 

--count sql agent steps that do not have a success status code (t2.run_status 0=failed, 1=success, 2=retry, 3=cancelled, 4=in progress)
set @sqlagent_job_fail_count = 0

set @sqlagent_job_fail_count = (
select count(t2.run_status)
FROM    msdb.dbo.sysjobs t1
JOIN    msdb.dbo.sysjobhistory t2
ON t1.job_id = t2.job_id 
--Join to pull most recent job activity per job, not job step
JOIN    msdb.dbo.sysjobsteps t4
on t2.job_id = t4.job_id and t2.step_name = t4.step_name
JOIN    (
         SELECT  TOP 1
       		   t1.job_id
                    ,t1.start_execution_date
                    ,t1.stop_execution_date
            FROM    msdb.dbo.sysjobactivity t1
            --If no job_id detected, return last run job
            WHERE   t1.job_id = COALESCE(@job_id,t1.job_id)
            ORDER 
            BY      last_executed_step_date DESC
            ) t3
            --Filter on the most recent job_id
            ON t1.job_id = t3.job_Id
            --Filter out job steps that do not fall between start_execution_date and stop_execution_date
            AND CONVERT(DATETIME, CONVERT(CHAR(8), t2.run_date, 112) + ' ' 
            + STUFF(STUFF(RIGHT('000000' + CONVERT(VARCHAR(8), t2.run_time), 6), 5, 0, ':'), 3, 0, ':'), 121)  
            BETWEEN t3.start_execution_date AND COALESCE(t3.stop_execution_date,current_timestamp)
			AND t2.run_status <> 1
			)


--compose body from sql agent query subject depends on non zero fail counts
set @bodyHTML = @bodyHTML_SQLAGENT

select @email_subject_status = case when (@sqlagent_job_fail_count = 0) then 'SUCCESS' else (CONVERT(nvarchar(3), @sqlagent_job_fail_count) + ' SQL Agent Exception(s)') end
set @email_subject =  @email_subject_status + ' ' + @job_name + ' SQL Agent Status Report'


--send email
exec msdb.dbo.sp_send_dbmail 
@profile_name='default mail',
@recipients = @email_dl,
@subject = @email_subject,
@body = @bodyHTML,
@body_format = 'HTML';

END
GO
