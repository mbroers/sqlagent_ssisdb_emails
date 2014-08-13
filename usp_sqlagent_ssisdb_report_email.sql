iUSE [msdb]
GO

/****** Object:  StoredProcedure [dbo].[usp_sqlagent_ssisdb_report_email]    Script Date: 8/13/2014 4:32:14 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[usp_sqlagent_ssisdb_report_email]  @job_id uniqueidentifier, @email_dl nvarchar(255)
AS
BEGIN

-- variables
declare @email_subject nvarchar(255),
@sqlagent_job_fail_count int,
@ssis_job_fail_count int,
@email_subject_status nvarchar(255),
@bodyHTML_SQLAGENT nvarchar(MAX), 
@bodyHTML_SSISDB nvarchar(MAX), 
@singleHTML_SSISDB nvarchar(MAX),
@HTML_SSISDB nvarchar(MAX),
@bodyHTML nvarchar(MAX),
@job_name nvarchar(255)

--determine job from guid job id
select @job_name = name from msdb.dbo.sysjobs where job_id=@job_id

--generate sql agent status report body
set @bodyHTML_SQLAGENT = N'<body background="#eff4f3">'
	+ N'<font face="Arial, Helvetica, sans-serif">'
	+ N'<table style="border:0px;border-collapse:collapse;width:900px;margin:0px auto">'
	+ N'<caption style="text-align:left;font-size:16px;font-weight:bold;color:#5e6e65;text-transform:uppercase;padding:10px 0px 0px 0px">'
	+ @job_name + ' on ' + reverse(left((reverse(@@servername)),(charindex('-',reverse(@@servername))-1))) + '</caption></table>'
	+ N'<table style="border:0px solid black;border-top:3px solid #ff9900;border-collapse:collapse;width:900px;margin:0px auto">'
	+ N'<caption style="text-align:left;font-size:13px;font-weight:bold;color:#5e6e65;padding: 15px 0px 5px 0px">SQL AGENT STEPS</caption>'
	+ N'<tr bgcolor="#AFDACF">'
	+ N'<th style="padding: 6px 6px 6px 12px;font-size:12px;text-transform:uppercase;color:#5e6e65;border-bottom:1px solid #cdcdcd">Status</th>'
	+ N'<th style="padding: 6px 6px 6px 12px;font-size:12px;text-transform:uppercase;color:#5e6e65;border-bottom:1px solid #cdcdcd">Step</th>'
	+ N'<th style="padding: 6px 6px 6px 12px;font-size:12px;text-transform:uppercase;color:#5e6e65;border-bottom:1px solid #cdcdcd">Step Name</th>'
	+ N'<th style="padding: 6px 6px 6px 12px;font-size:12px;text-transform:uppercase;color:#5e6e65;border-bottom:1px solid #cdcdcd">Start</th>'
	+ N'<th style="padding: 6px 6px 6px 12px;font-size:12px;text-transform:uppercase;color:#5e6e65;border-bottom:1px solid #cdcdcd">Duration</th>'
	+ N'<th style="padding: 6px 6px 6px 12px;font-size:12px;text-transform:uppercase;color:#5e6e65;border-bottom:1px solid #cdcdcd">Type</th>'
	+ N'<th style="padding: 6px 6px 6px 12px;font-size:12px;text-transform:uppercase;color:#5e6e65;border-bottom:1px solid #cdcdcd">Command</th>'
	+ N'<th style="padding: 6px 6px 6px 12px;font-size:12px;text-transform:uppercase;color:#5e6e65;border-bottom:1px solid #cdcdcd">Message</th>'
	+ N'</tr>'
	+ CAST((SELECT CASE WHEN (ROW_NUMBER() OVER (ORDER BY [t2].[step_id]))%2=1 THEN '#FFF' ELSE '#f7f7f7' END AS [tr/@bgcolor]
	, CASE t2.run_status WHEN 0 THEN '#FF4D4D' WHEN 1 THEN '#8DE28D' ELSE '#FFFF66' END AS [td/@bgcolor]
	, 'padding:5px 12px 7px 12px;font-size:11px;color:#5e6e65;border-bottom:1px solid #cdcdcd;' as [td/@style]
	, CONVERT(VARCHAR(14), (CASE [t2].[run_status] WHEN 0 THEN 'Failed' WHEN 1 THEN 'Succeeded' WHEN 2 THEN 'Retry' WHEN 3 THEN 'Cancelled'  WHEN 4 THEN 'In Progress' END)) as [td]
	, ''
	, 'padding:5px 6px 7px 6px;font-size:11px;color:#5e6e65;border-bottom:1px solid #cdcdcd;text-align:center' as [td/@style]
	, td = CONVERT(VARCHAR(255),[t2].[step_id]) 
	, ''
	, 'padding:5px 6px 7px 12px;font-size:11px;color:#5e6e65;border-bottom:1px solid #cdcdcd;' as [td/@style]
	, td = CONVERT(VARCHAR(255),[t2].[step_name]) 
	, ''
	, 'padding:5px 6px 7px 12px;font-size:11px;color:#5e6e65;border-bottom:1px solid #cdcdcd;' as [td/@style]
	, td = CONVERT(VARCHAR(255),msdb.dbo.agent_datetime(t2.run_date, t2.run_time))
	, ''
	, 'padding:5px 6px 7px 12px;font-size:11px;color:#5e6e65;border-bottom:1px solid #cdcdcd;text-align:center' as [td/@style]
	, td = CONVERT(VARCHAR(255),stuff(stuff(replace(str(run_duration,6,0),' ','0'),3,0,':'),6,0,':')) 
	, ''
	, 'padding:5px 6px 7px 12px;font-size:11px;color:#5e6e65;border-bottom:1px solid #cdcdcd;text-align:center' as [td/@style]
	, td = CONVERT(VARCHAR(255),[t4].[subsystem]) 
	, ''
	, 'padding:5px 6px 7px 12px;font-size:11px;color:#5e6e65;border-bottom:1px solid #cdcdcd;' as [td/@style]
	, td = CONVERT(VARCHAR(MAX), (case when t4.subsystem = 'SSIS' then reverse(left(reverse(left(t4.command,charindex('.dtsx',t4.command)+4)),charindex('\',reverse(left(t4.command,charindex('.dtsx',t4.command)+4)))-1)) else t4.command end)) 
	, ''
	, 'padding:5px 6px 7px 12px;font-size:11px;color:#5e6e65;border-bottom:1px solid #cdcdcd;' as [td/@style]
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


--get exeuction_ids and pass to ssisdb execution overview proc
select distinct e.execution_id
into #execution_ids
from ssisdb.catalog.executions e
join
	(SELECT  
            todatetimeoffset(msdb.dbo.agent_datetime(t2.run_date, t2.run_time), '-05:00') as 'start_time'
			,todatetimeoffset(dateadd(second, t2.run_duration, msdb.dbo.agent_datetime(t2.run_date, t2.run_time)), '-05:00') as 'end_time'
            ,reverse(left(reverse(left(t4.command,charindex('.dtsx',t4.command)+4)),charindex('\',reverse(left(t4.command,charindex('.dtsx',t4.command)+4)))-1)) as 'package_name'
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
			where t4.subsystem='SSIS') s
on e.package_name = s.package_name
and e.start_time > s.start_time
and e.end_time < s.end_time
order by e.execution_id desc
--possible null issues might need isnull or coalesce

--loop through proc that returns ssisdb info using a cursor, passing execution_ids, and tallying errors
declare @eid int
declare @ssis_job_fail_count_tally int
set @ssis_job_fail_count_tally = 0

declare cur cursor for select execution_id from #execution_ids order by execution_id desc
open cur

fetch next from cur into @eid
while @@FETCH_STATUS = 0 begin
  exec msdb.dbo.usp_ssisdb_execution_overview_errors_for_report_email @execution_id = @eid,  @HTML_SSISDB = @singleHTML_SSISDB OUTPUT, @ssis_job_fail = @ssis_job_fail_count OUTPUT
  set @bodyHTML_SSISDB = @singleHTML_SSISDB + isnull(@bodyHTML_SSISDB,'')
  set @ssis_job_fail_count_tally = @ssis_job_fail_count + @ssis_job_fail_count_tally 
  fetch next from cur into @eid
end

close cur
deallocate cur

--cleanup
drop table #execution_ids


--compose body from sql agent query and stored proc result from ssisdb, subject depends on non zero fail counts
set @bodyHTML = @bodyHTML_SQLAGENT + isnull(@bodyHTML_SSISDB,'')

select @email_subject_status = case when (@sqlagent_job_fail_count = 0 AND @ssis_job_fail_count_tally = 0) then 'SUCCESS' else (CONVERT(nvarchar(5), (@sqlagent_job_fail_count + @ssis_job_fail_count_tally)) + ' Exception(s)') end
set @email_subject =  @email_subject_status + ' ' + @job_name + ' SQL Agent Status Report'

--select @bodyHTML

--send email
exec msdb.dbo.sp_send_dbmail 
@profile_name='default mail',
@recipients = @email_dl,
@subject = @email_subject,
@body = @bodyHTML,
@body_format = 'HTML';

END

GO



