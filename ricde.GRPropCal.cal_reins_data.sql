CREATE OR REPLACE PACKAGE BODY GRPropCal is

  procedure cal_reins_data is
  begin
    insert into reins_temp_r_12003
      (taskid, policyno, endortimes, startdate)
    values
      ('reins_data' || to_char(sysdate, 'yyyymmdd'),
       'crt_redata_batch',
       'B',
       sysdate);
    COMMIT;
    --�ٱ��ֱ���������
    GRreDataCrt.crt_redata_batch;
    insert into reins_temp_r_12003
      (taskid, policyno, endortimes, startdate)
    values
      ('reins_data' || to_char(sysdate, 'yyyymmdd'),
       'crt_redata_batch',
       'E',
       sysdate);
    COMMIT;
    --�ٱ���������
    insert into reins_temp_r_12003
      (taskid, policyno, endortimes, startdate)
    values
      ('reins_data' || to_char(sysdate, 'yyyymmdd'),
       'prop_cal_batch',
       'B',
       sysdate);
    COMMIT;
    grpropcal.prop_cal_batch;
    insert into reins_temp_r_12003
      (taskid, policyno, endortimes, startdate)
    values
      ('reins_data' || to_char(sysdate, 'yyyymmdd'),
       'prop_cal_batch',
       'E',
       sysdate);
    COMMIT;
    --�ٱ��ֱ����ݸ���
    insert into reins_temp_r_12003
      (taskid, policyno, endortimes, startdate)
    values
      ('reins_data' || to_char(sysdate, 'yyyymmdd'),
       'prop_check_batch',
       'B',
       sysdate);
    COMMIT;
    grpropcal.prop_check_batch;
    insert into reins_temp_r_12003
      (taskid, policyno, endortimes, startdate)
    values
      ('reins_data' || to_char(sysdate, 'yyyymmdd'),
       'prop_check_batch',
       'E',
       sysdate);
    COMMIT;
    --��Լ�ش��ⰸȡ��
    insert into reins_temp_r_12003
      (taskid, policyno, endortimes, startdate)
    values
      ('reins_data' || to_char(sysdate, 'yyyymmdd'),
       'send_email_message',
       'B',
       sysdate);
    COMMIT;
    GRreDataCrt.send_email_message;
    insert into reins_temp_r_12003
      (taskid, policyno, endortimes, startdate)
    values
      ('reins_data' || to_char(sysdate, 'yyyymmdd'),
       'send_email_message',
       'E',
       sysdate);
    COMMIT;
    --�Ǳ�������׼��
    insert into reins_temp_r_12003
      (taskid, policyno, endortimes, startdate)
    values
      ('reins_data' || to_char(sysdate, 'yyyymmdd'),
       'XLCal_step1',
       'B',
       sysdate);
    COMMIT;
    GRNpropcal.XLCal_step1;
    insert into reins_temp_r_12003
      (taskid, policyno, endortimes, startdate)
    values
      ('reins_data' || to_char(sysdate, 'yyyymmdd'),
       'XLCal_step1',
       'E',
       sysdate);
    COMMIT;
    --�Ǳ�������ڶ���
    insert into reins_temp_r_12003
      (taskid, policyno, endortimes, startdate)
    values
      ('reins_data' || to_char(sysdate, 'yyyymmdd'),
       'XLCal_step2',
       'B',
       sysdate);
    COMMIT;
    GRNpropcal.XLCal_step2;
    insert into reins_temp_r_12003
      (taskid, policyno, endortimes, startdate)
    values
      ('reins_data' || to_char(sysdate, 'yyyymmdd'),
       'XLCal_step2',
       'E',
       sysdate);
    COMMIT;
  end cal_reins_data;
/*prop_cal_batch �������㣬��������ҵ����,��δ����Ļ�����������¼���*/
procedure prop_cal_batch is
cursor cur_ply(p_date date) is
   select repolicyno,RecTimes,ReinsType,StartDate,
          ReStartDate,Status
     from REINS_REPOLICY
    where status in ('0','2')
      and restartdate<p_date
      and uwenddate<p_date;
rec_ply cur_ply%rowtype;

cursor cur_edr(p_date date) is
   select repolicyno,reendortimes,RecTimes,ReinsType,StartDate,
          ReStartDate,Status,riskcode
     from REINS_REENDOR
    where status in ('0','2')
      and restartdate<p_date
      and uwenddate<p_date
    order by policyno,endortimes;
    --order by repolicyno,restartdate,reendortimes;
rec_edr cur_edr%rowtype;

cursor cur_clm is
   select reclaimno,RecTimes,ReinsType,Status
     from REINS_RECLAIM
    where status in ('0','2');
rec_clm cur_clm%rowtype;

cursor cur_OSclm is
   select OSreclaimno,ReinsType,Status
     from REINS_OS_RECLAIM
    where status in ('0','2');
rec_OSclm cur_OSclm%rowtype;

v_date date;
v_message_code varchar2(20);
v_message_Desc varchar2(60);
begin
 insert into reins_temp_r_12003 (taskid,policyno,startdate)values('prop_cal_batch','prop_cal_batch'||to_char(sysdate,'yyyymmdd'),sysdate);
 commit;
 v_date:=last_day(grbilldeal.get_bill_date(null));
 /*if v_date>=sysdate then
    v_date:=sysdate;
 end if;*/
  --�����𱣵ĵ����ɼ���
  v_date:=to_date(to_char(v_date,'yyyy')||'-12-31','yyyy-mm-dd')+1;
----dbms_output.put_line('================='||to_char(v_date,'yyyy-mm-dd'));
  for rec_ply in cur_ply(v_date) loop
    --ɾ��������־
    delete from REINS_POLICY_ERR_LOG
      where repolicyno = rec_ply.repolicyno
        and rectimes = rec_ply.rectimes
        and errtype = 'B1';
    g_errmsg :=null;
    commit;
   if rec_ply.reinstype='0' then
      quota_ply_cal(rec_ply.repolicyno,
                  rec_ply.rectimes,
                  v_message_code,
                  v_message_desc);
   elsif rec_ply.reinstype='1' then
      fac_ply_cal(rec_ply.repolicyno,
                  rec_ply.rectimes,
                  v_message_code,
                  v_message_desc);
   else
      nfac_ply_cal(rec_ply.repolicyno,
                  rec_ply.rectimes,
                  v_message_code,
                  v_message_desc);
   end if;
  end loop;

  for rec_edr in cur_edr(v_date) loop
    --ɾ��������־
    delete from REINS_ENDOR_ERR_LOG
      where repolicyno = rec_edr.repolicyno
        and reendortimes = rec_edr.reendortimes
        and rectimes = rec_edr.rectimes
        and errtype = 'B2';
    g_errmsg :=null;
    commit;
   if rec_edr.reinstype='0' then
      /*if rec_edr.riskcode='1703' then
        quota_edr_longrisk_cal(rec_edr.repolicyno,
                    rec_edr.reendortimes,
                    rec_edr.rectimes,
                    v_message_code,
                    v_message_desc);
      else*/
        quota_edr_cal(rec_edr.repolicyno,
                    rec_edr.reendortimes,
                    rec_edr.rectimes,
                    v_message_code,
                    v_message_desc);
      /* end if;*/
   elsif rec_edr.reinstype='1' then
      fac_edr_cal(rec_edr.repolicyno,
                  rec_edr.reendortimes,
                  rec_edr.rectimes,
                  v_message_code,
                  v_message_desc);
   else
      nfac_edr_cal(rec_edr.repolicyno,
                  rec_edr.reendortimes,
                  rec_edr.rectimes,
                  v_message_code,
                  v_message_desc);
   end if;
  end loop;

  for rec_clm in cur_clm loop
   --ɾ��������־
   delete from REINS_CLAIM_ERR_LOG
   where reclaimno = rec_clm.reclaimno
     and rectimes = rec_clm.rectimes
     and errtype = 'B3';
    g_errmsg :=null;
    commit;
   if rec_clm.reinstype='0' then
      quota_clm_cal(rec_clm.reclaimno,
                  rec_clm.rectimes,
                  v_message_code,
                  v_message_desc);
   elsif rec_clm.reinstype='1' then
      fac_clm_cal(rec_clm.reclaimno,
                  rec_clm.rectimes,
                  v_message_code,
                  v_message_desc);
   else
      nfac_clm_cal(rec_clm.reclaimno,
                  rec_clm.rectimes,
                  v_message_code,
                  v_message_desc);
   end if;
  end loop;

  for rec_OSclm in cur_OSclm loop
   delete from REINS_OS_CLAIM_ERR_LOG
    where OSReClaimNo = rec_OSclm.OSReClaimNo
      and errtype = 'B4';
    g_errmsg :=null;
    commit;
   if(rec_OSclm.reinstype='0') then
     quota_OSclm_cal(rec_OSclm.OSreclaimno,
                    v_message_code,
                    v_message_desc);
   elsif(rec_OSclm.reinstype='1') then
     fac_OSclm_cal(rec_OSclm.OSreclaimno,
                    v_message_code,
                    v_message_desc);
   else
     nfac_OSclm_cal(rec_OSclm.OSreclaimno,
                    v_message_code,
                    v_message_desc);
   end if;
  end loop;
  update reins_temp_r_12003
     set enddate = sysdate
   where taskid = 'prop_cal_batch'
     and policyno = 'prop_cal_batch' || to_char(sysdate, 'yyyymmdd');
  commit;
end  prop_cal_batch;

/*prop_check_batch��������,��������ҵ����,��δ���˵ķֱ�/��/�ⰸ�ļ���������*/
procedure prop_check_batch is
cursor cur_ply(p_date date) is
   select repolicyno,RecTimes,ReinsType
     from REINS_REPOLICY
    where status ='1'
      and checkind='0'
      and reinstype='0'
      and restartdate<p_date
      and uwenddate<p_date;
rec_ply cur_ply%rowtype;

cursor cur_edr(p_date date) is
   select repolicyno,reendortimes,RecTimes,ReinsType
     from REINS_REENDOR
    where status ='1'
      and checkind='0'
      and reinstype='0'
      and restartdate<p_date
      and uwenddate<p_date;
rec_edr cur_edr%rowtype;

cursor cur_clm(p_date date) is
   select reclaimno,RecTimes,ReinsType
     from REINS_RECLAIM
    where status ='1'
      and checkind='0'
      and reinstype='0'
      and uwenddate<p_date;
rec_clm cur_clm%rowtype;
v_date date;
messagecode varchar2(20);
begin
  insert into reins_temp_r_12003 (taskid,policyno,startdate)values('prop_check_batch','prop_check_batch'||to_char(sysdate,'yyyymmdd'),sysdate);
 commit;
  --���뵱ǰ�ʵ��ڵĲ��ܸ���
  v_date:=last_day(grbilldeal.get_bill_date(null))+1;

  for rec_ply in cur_ply(v_date) loop
    delete from REINS_POLICY_ERR_LOG
     where repolicyno = rec_ply.repolicyno
       and rectimes = rec_ply.rectimes
       and errtype = 'C1';
    g_errmsg := null;
    commit;

     repolicy_check(rec_ply.repolicyno,rec_ply.rectimes);
     commit;
  end loop;

  for rec_edr in cur_edr(v_date) loop
    delete from REINS_ENDOR_ERR_LOG
     where repolicyno = rec_edr.repolicyno
       and reendortimes = rec_edr.reendortimes
       and rectimes = rec_edr.rectimes
       and errtype = 'C2';
    g_errmsg := null;
    commit;

     reendor_check(rec_edr.repolicyno,rec_edr.reendortimes,rec_edr.rectimes);
     commit;
  end loop;

  for rec_clm in cur_clm(v_date) loop
      delete from REINS_CLAIM_ERR_LOG
       where reclaimno = rec_clm.reclaimno
         and rectimes = rec_clm.rectimes
         and errtype = 'C3';
      g_errmsg := null;
      commit;

     reClaim_check(rec_clm.reclaimno,rec_clm.recTimes,messagecode);
     commit;
  end loop;
  --�ٷ��Ѿ��ⰸת����
  ricde.grbilldeal.fac_bill_batch;
   update reins_temp_r_12003
     set enddate = sysdate
   where taskid = 'prop_check_batch'
     and policyno = 'prop_check_batch' || to_char(sysdate, 'yyyymmdd');
  commit;
end prop_check_batch;

procedure data_check_batch(
                 p_accym in varchar2,
                 p_flag  in varchar2
) is
  /*p_flag[10]='0000000000': ÿһλ�ֱ��ʾ���£�
    ��1λ�����ɷֵ�����©���
    ��2λ���ֱ�������©���
    ��3λ���ֱ����������©���
    ��4λ���������ֱ����������
    ��5λ���ⰸ��̯���������
    ʣ��λ:����*/

  --�ֱ�������©���
  cursor cply(p_date1 date, p_date2 date) is
   select a.repolicyno,
          a.rectimes,
          a.comcode,
          a.reriskcode,
          a.combineind,
          b.ttyid,
          b.ttytype,
          b.ricurr,
          b.exchrate,
          b.sharerate,
          a.pml,
          b.risum,
          a.grsprem,
          b.grsprem riprem,
          --add by 2013-07-11 �Ƿ�ũ������,��ҵ������
          a.channel_class channelClass,
          a.industry_class industryClass
     from REINS_REPOLICY a, REINS_REPLY_SHARE b
    where a.repolicyno = b.repolicyno
      and a.rectimes = b.rectimes
      and a.checkdate >= p_date1
      and a.checkdate < p_date2
      and a.status = '1'
      and a.checkind = '1'
      and b.ttytype not in ('81', '82', '91', '92')
    order by a.repolicyno, a.rectimes;
  ply cply%rowtype;

  cursor cedr(p_date1 date, p_date2 date) is
   select a.repolicyno,
          a.reendortimes,
          a.rectimes,
          a.comcode,
          a.reriskcode,
          a.combineind,
          b.ttyid,
          b.ttytype,
          b.ricurr,
          b.exchrate,
          b.sharerate,
          a.pml,
          b.risum,
          a.grsprem,
          b.grsprem riprem,
          a.chgpml,
          b.chgrisum,
          a.chggrsprem,
          b.chggrsprem chgriprem,
           --add by 2013-07-11 �Ƿ�ũ������,��ҵ������
          c.channel_class channelClass,
          c.industry_class industryClass
     from REINS_REENDOR a, REINS_REENDR_SHARE b, REINS_REPOLICY c
    where a.repolicyno = b.repolicyno
      and a.reendortimes = b.reendortimes
      and a.rectimes = b.rectimes
      and a.checkdate >= p_date1
      and a.checkdate < p_date2
      and a.status = '1'
      and a.checkind = '1'
      and a.repolicyno=c.repolicyno
      and a.rectimes=c.rectimes
      --and (a.pml<>0 or a.chgpml<>0)
      and b.ttytype not in ('81', '82', '91', '92')
    order by a.repolicyno, a.reendortimes, a.rectimes;
  edr cedr%rowtype;

  cursor cclm(p_date1 date, p_date2 date) is
   select a.reclaimno,
          a.rectimes,
          a.paidsum,
          a.comcode,
          a.reriskcode,
          b.ttyid,
          b.ttytype,
          b.exchrate,
          b.sharerate,
          b.paidsum ripaidsum,
          --add by 2013-07-11 �Ƿ�ũ������,��ҵ������
          c.channel_class channelClass,
          c.industry_class industryClass
     from REINS_RECLAIM a, REINS_RECLAIM_SHARE b, REINS_REPOLICY c
    where a.reclaimno = b.reclaimno
      and a.rectimes = b.rectimes
      and a.status = '1'
      and a.checkind = '1'
      and b.paydate >= p_date1
      and b.paydate < p_date2
      and b.ttytype not in ('81', '82', '91', '92')
      and a.repolicyno=c.repolicyno
      and a.rectimes=c.rectimes
   order by a.reclaimno,a.rectimes;
  clm     cclm%rowtype;

  cursor cosclm(p_date date) is
   select a.osreclaimno,
          a.ossum,
          a.comcode,
          a.reriskcode,
          b.ttyid,
          b.ttytype,
          b.exchrate,
          b.sharerate,
          b.ossum riossum,
          --add by 2013-07-11 �Ƿ�ũ������,��ҵ������
          c.channel_class channelClass,
          c.industry_class industryClass
     from REINS_OS_RECLAIM a, REINS_OS_RECLAIM_SHARE b, REINS_REPOLICY c
    where a.osreclaimno = b.osreclaimno
      and a.reportdate < p_date
      and b.ttytype not in ('81', '82', '91', '92')
      and a.repolicyno=c.repolicyno
    order by a.osreclaimno;
  osclm     cosclm%rowtype;

  v_date1      date;
  v_date2      date;
  v_risum      number:=0;
  v_riprem     number:=0;
  v_reinsrate  number:=0;
  v_limitvalue number:=0;
  v_line       number:=0;
  v_exchrate   number:=0;

  v_openind    varchar2(1);
  v_char       varchar2(1);
  v_DealFlg    varchar2(1);
  v_errcde     varchar2(5):='99';
  v_errmsg     varchar2(200);
begin
  if substr(p_flag,1,1) = '1' then
    delete from REINS_POLICY_ERR_LOG where errtype like 'D%' and errcode='99';
    delete from REINS_ENDOR_ERR_LOG where errtype like 'D%' and errcode='99';
    delete from REINS_CLAIM_ERR_LOG where errtype like 'D%' and errcode='99';
    delete from REINS_OS_CLAIM_ERR_LOG where errtype like 'D%' and errcode='99';
  end if;
  if substr(p_flag,2,1) = '1' then
    delete from REINS_POLICY_ERR_LOG where errtype like 'E%' and errcode='99';
    delete from REINS_ENDOR_ERR_LOG where errtype like 'E%' and errcode='99';
    delete from REINS_CLAIM_ERR_LOG where errtype like 'E%' and errcode='99';
    delete from REINS_OS_CLAIM_ERR_LOG where errtype like 'E%' and errcode='99';
  end if;
  if substr(p_flag,3,1) = '1' then
    delete from REINS_POLICY_ERR_LOG where errtype like 'F%' and errcode='99';
    delete from REINS_ENDOR_ERR_LOG where errtype like 'F%' and errcode='99';
    delete from REINS_CLAIM_ERR_LOG where errtype like 'F%' and errcode='99';
    delete from REINS_OS_CLAIM_ERR_LOG where errtype like 'F%' and errcode='99';
  end if;
  if substr(p_flag,4,1) = '1' then
    delete from REINS_POLICY_ERR_LOG where errtype like 'G%' and errcode='99';
    delete from REINS_ENDOR_ERR_LOG where errtype like 'G%' and errcode='99';
  end if;
  if substr(p_flag,5,1) = '1' then
    delete from REINS_CLAIM_ERR_LOG where errtype like 'G%' and errcode='99';
    delete from REINS_OS_CLAIM_ERR_LOG where errtype like 'G%' and errcode='99';
  end if;
  commit;

  v_date1 := grbilldeal.get_bill_date(null);
  v_date2 := last_day(v_date1)+1;
  --��δָ���ʵ��ڣ���Ĭ�ϼ�鵱ǰ�����µ�����
  --���Ҷ�����©������Ŀ�����������ĩ֮����ܼ��
  if p_accym is null and sysdate>=v_date2 then
     v_DealFlg := '1';
  --��ָ���ʵ��ڣ�����ָ���ʵ��µ�����
  elsif p_accym is not null then
     v_DealFlg := '1';
     v_date1 := to_date(p_accym||'01','yyyymmdd');
     v_date2 := last_day(to_date(p_accym,'yyyymm'))+1;
  end if;

  if v_DealFlg = '1' then
    ------���ɷֵ�����©���------
    if substr(p_flag,1,1) = '1' then
      v_errmsg := '����Σ�յ�λδ���ɷֵ���';
      insert into REINS_POLICY_ERR_LOG
        (ErrType,
         Policyno,
         Dangerunitno,
         Riskcode,
         Reriskcode,
         ErrCode,
         Errmsg)
        select 'D1',
               Policyno,
               Dangerunitno,
               Riskcode,
               Reriskcode,
               v_errcde,
               v_errmsg
          from REINS_POLICY_UNIT
         where REINSUREIND = '0'
           and startdate < v_date2
           and uwenddate < v_date2;

      v_errmsg := '����Σ�յ�λδ���ɷ�������';
      insert into REINS_ENDOR_ERR_LOG
        (ErrType,
         Policyno,
         Endortimes,
         Dangerunitno,
         Riskcode,
         Reriskcode,
         ErrCode,
         Errmsg)
        select 'D2',
               Policyno,
               Endortimes,
               Dangerunitno,
               Riskcode,
               Reriskcode,
               v_errcde,
               v_errmsg
          from REINS_ENDOR_UNIT
         where REINSUREIND = '0'
           and startdate < v_date2
           and uwenddate < v_date2;

      v_errmsg := '�Ѿ�Σ�յ�λδ���ɷ��ⰸ��';
      insert into REINS_CLAIM_ERR_LOG
        (errtype,
         payno,
         lossseqno,
         dangerunitno,
         riskcode,
         reriskcode,
         ErrCode,
         errmsg)
        select 'D3',
               payno,
               lossseqno,
               dangerunitno,
               riskcode,
               reriskcode,
               v_errcde,
               v_errmsg
          from REINS_CLAIM_UNIT
         where REINSUREIND = '0'
           and UWENDDATE < v_date2;

      v_errmsg := 'δ��Σ�յ�λδ����δ�����ⰸ��';
      insert into REINS_OS_CLAIM_ERR_LOG
        (errtype, claimno, dangerunitno, riskcode, reriskcode, ErrCode, errmsg)
        select 'D4',
               claimno,
               dangerunitno,
               riskcode,
               reriskcode,
               v_errcde,
               v_errmsg
          from REINS_OS_CLAIM_UNIT
         where REINSUREIND = '0'
           and reportdate < v_date2;
    end if;


    ------�ֱ�������©���------
    if substr(p_flag,2,1) = '1' then
      v_errmsg := '�ֱ���δ����';
      insert into REINS_POLICY_ERR_LOG
        (errtype, repolicyno, rectimes, errcode, errmsg)
        select 'E1', repolicyno, rectimes, v_errcde, v_errmsg
          from REINS_REPOLICY
         where status in ('0', '2')
           and restartdate < v_date2
           and uwenddate < v_date2;

      v_errmsg := '������δ����';
      insert into REINS_ENDOR_ERR_LOG
        (errtype, repolicyno, reendortimes, rectimes, errcode, errmsg)
        select 'E2', repolicyno, reendortimes, rectimes, v_errcde, v_errmsg
          from REINS_REENDOR
         where status in ('0', '2')
           and restartdate < v_date2
           and uwenddate < v_date2;

      v_errmsg := '�Ѿ����ⰸδ����';
      insert into REINS_CLAIM_ERR_LOG
        (errtype, reclaimno, rectimes, errcode, errmsg)
        select 'E3', reclaimno, rectimes, v_errcde, v_errmsg
          from REINS_RECLAIM
         where status in ('0', '2')
           and uwenddate < v_date2;

      v_errmsg := 'δ�����ⰸδ����';
      insert into REINS_OS_CLAIM_ERR_LOG
        (errtype, osreclaimno, errcode, errmsg)
        select 'E4', osreclaimno, v_errcde, v_errmsg
          from REINS_OS_RECLAIM
         where status in ('0', '2');
    end if;


    ------�ֱ����������©���------
    if substr(p_flag,3,1) = '1' then
      v_errmsg := '�ֱ���δ����';
      insert into REINS_POLICY_ERR_LOG
        (errtype, repolicyno, rectimes, errcode, errmsg)
        select 'F1', repolicyno, rectimes, v_errcde, v_errmsg
          from REINS_REPOLICY
         where status = '1'
           and checkind = '0'
           and restartdate < v_date2
           and uwenddate < v_date2;

      v_errmsg := '������δ����';
      insert into REINS_ENDOR_ERR_LOG
        (errtype, repolicyno, reendortimes, rectimes, errcode, errmsg)
        select 'F2', repolicyno, reendortimes, rectimes, v_errcde, v_errmsg
          from REINS_REENDOR
         where status = '1'
           and checkind = '0'
           and restartdate < v_date2
           and uwenddate < v_date2;

      v_errmsg := '�Ѿ����ⰸδ����';
      insert into REINS_CLAIM_ERR_LOG
        (errtype, reclaimno, rectimes, errcode, errmsg)
        select 'F3', reclaimno, rectimes, v_errcde, v_errmsg
          from REINS_RECLAIM
         where status = '1'
           and checkind = '0'
           and uwenddate < v_date2;
    end if;

  end if;

  ------�������ֱ����������------
  if substr(p_flag,4,1) = '1' then
    --����
    for ply in cply(v_date1,v_date2) loop
    begin
       v_errmsg := '��ԼID('||ply.ttyid||'):';
       select a.reinsrate,a.limitvalue,nvl(a.lines,0),
              get_exchrate(ply.ricurr,a.currency,c.startdate)
         into v_reinsrate,v_limitvalue,v_line,v_exchrate
         from REINS_TREATY c,REINS_TTY_SECT a,REINS_TTY_SECT_RISK b
        where c.ttyid = a.ttyid
          and a.ttyid = b.ttyid
          and a.sectno = b.sectno
          and a.ttyid = ply.ttyid
          and ply.comcode like b.comcode||'%'
          and b.reriskcode = ply.reriskcode
          --add by 2013-07-11 �Ƿ�ũ������,��ҵ������
          --and c.channel_class=ply.channelclass
          --and a.industry_class=ply.industryclass;
          --modified by huangxf 2013/08/23 ����ͨ�������
          AND c.channel_class = DECODE(c.channel_class, '*', '*', ply.channelClass)
          AND a.industry_class = DECODE(a.industry_class, '*', '*', ply.industryClass);

      if ply.ttytype in ('11','12','21') then
          if ply.ttytype='21' then
             select sum(risum/exchrate),sum(grsprem/exchrate)
               into v_risum,v_riprem
               from REINS_REPLY_SHARE
              where repolicyno=ply.repolicyno and rectimes=ply.rectimes
               and ttytype in('21','81');
            if abs(ply.risum/ply.exchrate-v_risum*v_reinsrate/100)>=1 then
               v_errmsg := v_errmsg||'���������ֳ��쳣';
               select '*' into v_char from dual where 1=0;
            end if;
          else
            select openind into v_openind from REINS_TTY_PLAN where ttyid=ply.ttyid;

            if v_openind='N' and abs(v_reinsrate-ply.sharerate)>=0.01 then
               v_errmsg := v_errmsg||'��ͨ�����ֳ������쳣';
               select '*' into v_char from dual where 1=0;
            end if;

            if abs(ply.risum-ply.pml*ply.sharerate*ply.exchrate/100)>=1 then
               v_errmsg := v_errmsg||'�ֳ������쳣';
               select '*' into v_char from dual where 1=0;
            end if;

            if abs(ply.riprem-ply.grsprem*ply.sharerate*ply.exchrate/100)>=0.1 then
               v_errmsg := v_errmsg||'�ֳ������쳣';
               select '*' into v_char from dual where 1=0;
            end if;
          end if;

      elsif ply.ttytype = '31' then
         if ply.combineind='1' then
             select sum(risum/exchrate),sum(grsprem/exchrate)
               into v_risum,v_riprem
               from REINS_REPLY_SHARE
              where repolicyno=ply.repolicyno and rectimes=ply.rectimes
               and ttytype in('21','81');

            if ply.risum/ply.exchrate - v_risum*v_line>=1 then
              v_errmsg := v_errmsg||'������Լ�޶�';
              select '*' into v_char from dual where 1=0;
            end if;
         else
            if ply.risum*v_exchrate - v_limitvalue>=1 then
              v_errmsg := v_errmsg||'������Լ�޶�';
              select '*' into v_char from dual where 1=0;
            end if;
         end if;
      end if;

    exception when others then
      rollback;
      insert into REINS_POLICY_ERR_LOG
          (errtype, repolicyno, rectimes, errcode, errmsg)
          select 'G1', ply.repolicyno, ply.rectimes, v_errcde, v_errmsg
            from dual;
      commit;
    end;
    end loop;

    v_errmsg := '�����ֳ�����֮�����ܱ��Ѳ�һ��';
    insert into REINS_POLICY_ERR_LOG
      (ErrType,
       Policyno,
       Dangerunitno,
       Riskcode,
       Reriskcode,
       ErrCode,
       Errmsg)
      select 'G1',
             Policyno,
             Dangerunitno,
             Riskcode,
             Reriskcode,
             v_errcde,
             v_errmsg
        from (select a.Policyno,
                     a.Dangerunitno,
                     a.Riskcode,
                     a.Reriskcode,
                     a.grsprem,
                     sum(b.grsprem/exchrate)
                from REINS_REPOLICY a, REINS_REPLY_SHARE b
               where a.repolicyno = b.repolicyno
                 and a.rectimes = b.rectimes
                 and a.status = '1'
                 --and a.checkind = '1'
                 and a.uwenddate >= v_date1
                 and a.uwenddate < v_date2
               group by a.Policyno,
                        a.Dangerunitno,
                        a.Riskcode,
                        a.Reriskcode,
                        a.grsprem
              having abs(a.grsprem - sum(b.grsprem/exchrate)) >= 1);
    commit;

    --����
    for edr in cedr(v_date1,v_date2) loop
    begin
       v_errmsg := '��ԼID('||edr.ttyid||'):';
       select a.reinsrate,a.limitvalue,nvl(a.lines,0),
              get_exchrate(edr.ricurr,a.currency,c.startdate)
         into v_reinsrate,v_limitvalue,v_line,v_exchrate
         from REINS_TREATY c,REINS_TTY_SECT a,REINS_TTY_SECT_RISK b
        where c.ttyid = a.ttyid
          and a.ttyid = b.ttyid
          and a.sectno = b.sectno
          and a.ttyid = edr.ttyid
          and edr.comcode like b.comcode||'%'
          and b.reriskcode = edr.reriskcode
          --add by 2013-07-11 �Ƿ�ũ������,��ҵ������
          --and c.channel_class=edr.channelclass
          --and a.industry_class=edr.industryclass;
          --modified by huangxf 2013/08/23 ����ͨ�������
          AND c.channel_class = DECODE(c.channel_class, '*', '*', edr.channelClass)
          AND a.industry_class = DECODE(a.industry_class, '*', '*', edr.industryClass);

      if edr.ttytype in ('11','12','21') then
          if edr.ttytype='21' then
             select sum(risum/exchrate),sum(grsprem/exchrate)
               into v_risum,v_riprem
               from REINS_REENDR_SHARE
              where repolicyno=edr.repolicyno and reendortimes=edr.reendortimes
               and rectimes=edr.rectimes and ttytype in('21','81');
            if abs(edr.risum/edr.exchrate-v_risum*v_reinsrate/100)>=1 then
               v_errmsg := v_errmsg||'���������ֳ��쳣';
               select '*' into v_char from dual where 1=0;
            end if;
          else
            if abs(edr.risum-edr.pml*edr.sharerate*edr.exchrate/100)>=1 then
               v_errmsg := v_errmsg||'�ֳ������쳣';
               select '*' into v_char from dual where 1=0;
            end if;

            if abs(edr.riprem-edr.grsprem*edr.sharerate*edr.exchrate/100)>=0.1 then
               v_errmsg := v_errmsg||'�ֳ������쳣';
               select '*' into v_char from dual where 1=0;
            end if;

            select openind into v_openind from REINS_TTY_PLAN where ttyid=edr.ttyid;
            if v_openind='N' then
              if abs(v_reinsrate-edr.sharerate)>=0.01 then
                 v_errmsg := v_errmsg||'��ͨ���������쳣';
                 select '*' into v_char from dual where 1=0;
              end if;

              if abs(edr.chgrisum-edr.chgpml*edr.sharerate*edr.exchrate/100)>=1 then
                 v_errmsg := v_errmsg||'�ֳ�����仯���쳣';
                 select '*' into v_char from dual where 1=0;
              end if;

              if abs(edr.chgriprem-edr.chggrsprem*edr.sharerate*edr.exchrate/100)>=0.1 then
                 v_errmsg := v_errmsg||'�ֳ����ѱ仯���쳣';
                 select '*' into v_char from dual where 1=0;
              end if;
            end if;
          end if;

      elsif edr.ttytype = '31' then
         if edr.combineind='1' then
             select sum(risum/exchrate),sum(grsprem/exchrate)
               into v_risum,v_riprem
               from REINS_REENDR_SHARE
              where repolicyno=edr.repolicyno and reendortimes=edr.reendortimes
               and rectimes=edr.rectimes and ttytype in('21','81');
            if edr.risum/edr.exchrate-v_risum*v_line>=1 then
              v_errmsg := v_errmsg||'������Լ�޶�';
              select '*' into v_char from dual where 1=0;
            end if;
         else
            if edr.risum*v_exchrate-v_limitvalue>=1 then
              v_errmsg := v_errmsg||'������Լ�޶�';
              select '*' into v_char from dual where 1=0;
            end if;
         end if;
      end if;

    exception when others then
      rollback;
      insert into REINS_ENDOR_ERR_LOG
          (errtype, repolicyno, reendortimes, rectimes, errcode, errmsg)
          select 'G2', edr.repolicyno, edr.reendortimes, edr.rectimes, v_errcde, v_errmsg
            from dual;
      commit;
    end;
    end loop;

    v_errmsg := '�����ֳ�����֮�����ܱ��Ѳ�һ��';
    insert into REINS_ENDOR_ERR_LOG
      (ErrType,
       Policyno,
       endortimes,
       Dangerunitno,
       Riskcode,
       Reriskcode,
       ErrCode,
       Errmsg)
      select 'G2',
             Policyno,
             endortimes,
             Dangerunitno,
             Riskcode,
             Reriskcode,
             v_errcde,
             v_errmsg
        from (select a.Policyno,
                     a.endortimes,
                     a.Dangerunitno,
                     a.Riskcode,
                     a.Reriskcode,
                     a.grsprem,
                     sum(b.grsprem/exchrate)
                from REINS_REENDOR a, REINS_REENDR_SHARE b
               where a.repolicyno = b.repolicyno
                 and a.reendortimes = b.reendortimes
                 and a.rectimes = b.rectimes
                 and a.status = '1'
                 --and a.checkind = '1'
                 and a.uwenddate >= v_date1
                 and a.uwenddate < v_date2
               group by a.Policyno,
                        a.endortimes,
                        a.Dangerunitno,
                        a.Riskcode,
                        a.Reriskcode,
                        a.grsprem
              having abs(a.grsprem - sum(b.grsprem/exchrate)) >= 1);

    v_errmsg := '�����ֳ����ѱ仯֮�����ܱ��ѱ仯��һ��';
    insert into REINS_ENDOR_ERR_LOG
      (ErrType,
       Policyno,
       endortimes,
       Dangerunitno,
       Riskcode,
       Reriskcode,
       ErrCode,
       Errmsg)
      select 'G2',
             Policyno,
             endortimes,
             Dangerunitno,
             Riskcode,
             Reriskcode,
             v_errcde,
             v_errmsg
        from (select a.Policyno,
                     a.endortimes,
                     a.Dangerunitno,
                     a.Riskcode,
                     a.Reriskcode,
                     a.chggrsprem,
                     sum(b.chggrsprem/exchrate)
                from REINS_REENDOR a, REINS_REENDR_SHARE b
               where a.repolicyno = b.repolicyno
                 and a.reendortimes = b.reendortimes
                 and a.rectimes = b.rectimes
                 and a.status = '1'
                 --and a.checkind = '1'
                 and a.uwenddate >= v_date1
                 and a.uwenddate < v_date2
               group by a.Policyno,
                        a.endortimes,
                        a.Dangerunitno,
                        a.Riskcode,
                        a.Reriskcode,
                        a.chggrsprem
              having abs(a.chggrsprem - sum(b.chggrsprem/exchrate)) >= 1);
    commit;

  end if;


  ------�ⰸ��̯���������------
  if substr(p_flag,5,1) = '1' then
    --�Ѿ�
    for clm in cclm(v_date1, v_date2) loop
    begin
       if clm.ttytype in ('11','12') then
         v_errmsg := '��ԼID('||clm.ttyid||'):';
         select a.reinsrate
           into v_reinsrate
           from REINS_TTY_SECT a,REINS_TTY_SECT_RISK b
          where a.ttyid = b.ttyid
            and a.sectno = b.sectno
            and a.ttyid = clm.ttyid
            and clm.comcode like b.comcode||'%'
            and b.reriskcode = clm.reriskcode
            --add by 2013-07-11 �Ƿ�ũ������,��ҵ������
            --modified by huangxf 2013/08/24 ����ͨ�������
            --and a.industry_class = clm.industryclass;
            AND a.industry_class = DECODE(a.industry_class, '*', '*', clm.industryclass);

         select openind into v_openind from REINS_TTY_PLAN where ttyid=clm.ttyid;

         if v_openind='N' and abs(v_reinsrate-clm.sharerate)>=0.01 then
           v_errmsg := v_errmsg||'̯�ر����쳣';
           select '*' into v_char from dual where 1=0;
         end if;
       end if;

       if abs(clm.ripaidsum-clm.paidsum*clm.sharerate*clm.exchrate/100)>=0.5 then
         v_errmsg := v_errmsg||'̯������쳣';
         select '*' into v_char from dual where 1=0;
       end if;

    exception when others then
      rollback;
      insert into REINS_CLAIM_ERR_LOG
        (errtype, reclaimno, rectimes, errcode, errmsg)
        select 'G3', clm.reclaimno, clm.rectimes, v_errcde, v_errmsg
          from dual;
      commit;
    end;
    end loop;

    --δ��
    for osclm in cosclm(v_date2) loop
    begin
       if clm.ttytype in ('11','12') then
         v_errmsg := '��ԼID('||osclm.ttyid||'):';
         select a.reinsrate
           into v_reinsrate
           from REINS_TTY_SECT a,REINS_TTY_SECT_RISK b
          where a.ttyid = b.ttyid
            and a.sectno = b.sectno
            and a.ttyid = osclm.ttyid
            and osclm.comcode like b.comcode||'%'
            and b.reriskcode = osclm.reriskcode
            --add by 2013-07-11 �Ƿ�ũ������,��ҵ������
            --and a.industry_class = osclm.industryclass;
            --modified by huangxf 2013/08/24 ����ͨ�������
            AND a.industry_class = DECODE(a.industry_class, '*', '*', osclm.industryclass);

         select openind into v_openind from REINS_TTY_PLAN where ttyid=osclm.ttyid;

         if v_openind='N' and abs(v_reinsrate-osclm.sharerate)>=0.01 then
           v_errmsg := v_errmsg||'Ӧ̯�����쳣';
           select '*' into v_char from dual where 1=0;
         end if;
       end if;

       if abs(osclm.riossum-osclm.ossum*osclm.sharerate*osclm.exchrate/100)>=0.5 then
         v_errmsg := v_errmsg||'Ӧ̯����쳣';
         select '*' into v_char from dual where 1=0;
       end if;

    exception when others then
      rollback;
      insert into REINS_OS_CLAIM_ERR_LOG
        (errtype, osreclaimno, errcode, errmsg)
        select 'G4', osclm.osreclaimno,  v_errcde, v_errmsg
          from dual;
      commit;
    end;
    end loop;

  end if;

end ;

/*quota_ply_cal���ں�Լ�ֱ�������,������������ã��ɹ�����0,ʧ��Ϊ1*/
procedure quota_ply_cal(p_repolicyno in REINS_REPOLICY.repolicyno%type,
                        p_RecTimes   in REINS_REPOLICY.RecTimes%type,
                        p_message_code out varchar2,
                        p_message_desc out varchar2) is
cursor cur_repolicy is
  select * from REINS_REPOLICY
  where repolicyno=p_repolicyNo and RecTimes=p_recTimes
  and Status in ('0','2') --ֻ����δ����ʹ����
  for update ;
 rec_repolicy cur_repolicy%rowtype;
-- modify by wuwp �ֱ��ƻ������������¼�� begin
cursor cur_ttySectReins(p_ttyid varchar2,p_sectno varchar2) is
  select b.sharerate, a.rcrate ,c.ricommrateadjind
    from REINS_TTY_SECT_REINS a, REINS_TTY_REINS b,REINS_TTY_SECT c
   where a.ttyid = b.ttyid
     and a.ttyid = p_ttyid
     and a.sectno = p_sectno
     and a.ttyid = c.ttyid
     and a.sectno = c.sectno
     and a.reinscode = b.reinscode
     and nvl(a.brokercode,'*')=nvl(b.brokercode,'*');
  rec_ttySectReins cur_ttySectReins%rowtype;

cursor cur_PolicyPlanAdj(v_policyno Reins_Policy_Plan_Adj.Policyno%type,
                         v_dangerunitno Reins_Policy_Plan_Adj.Dangerunitno%type,
                         v_reriskcode Reins_Policy_Plan_Adj.Reriskcode%type,
                         v_riskcode Reins_Policy_Plan_Adj.Riskcode%type,
                         v_ttyid Reins_Policy_Plan_Adj.Ttyid%type,
                         v_endortimes Reins_Policy_Plan_Adj.Endortimes%type
                         ) is
  select a.openind, a.ricommrate
    from Reins_Policy_Plan_Adj a
   where a.policyno = v_policyno
     and a.dangerunitno = v_dangerunitno
     and a.reriskcode = v_reriskcode
     and a.riskcode = v_riskcode
     and a.ttyid = v_ttyid
     and a.endortimes = v_endortimes
     union
  select b.openind, b.ricommrate
    from reins_reply_plan_adj b
   where b.repolicyno = p_repolicyno
     and b.reendortimes = rec_repolicy.reendortimes
     and b.rectimes = p_RecTimes
     and b.ttyid = v_ttyid;
 rec_PolicyPlanAdj cur_PolicyPlanAdj%rowtype;
 -- modify by wuwp �ֱ��ƻ������������¼�� end
  v_Arr_Re_Abs Arr_Re_Abs;  --�ֳ����
  v_reply_para reply_para:=null;   --�ֱ�����

  v_short_rate REINS_REPLY_SHARE.ExchRate%type; --���ڷ��ʣ����Ƿֱ������뱣�����ڲ�һ�����
  v_char       char(1); --�����쳣��ֹ
  v_error_code varchar(2):='00';
  n_cnt1       number(3);

  v_OriPml     REINS_REPOLICY.pml%type:=0;
  v_CurPml     REINS_REPOLICY.pml%type:=0;
  v_AccOriPml  REINS_REPOLICY.pml%type:=0;
  v_AccCurPml  REINS_REPOLICY.pml%type:=0;
  v_tempPml  REINS_REPOLICY.pml%type:=0;
  v_tempRet  REINS_REPOLICY.pml%type:=0;
  v_tempCem  number(3):=0 ; --�ж��Ƿ�������
  v_Ricommrateadjind REINS_TTY_SECT.Ricommrateadjind%type:='N';  -- modify by wuwp �ֱ��ƻ������������¼��
begin
  open cur_repolicy;
   fetch cur_repolicy into rec_repolicy;
     if cur_repolicy%notfound then
      g_errcode:='B1101';
      g_errmsg :='�Ҳ����ֱ���';
      close cur_repolicy;
      v_error_Code:='01';
      select '*' into v_char from dual where 1=2;
     end if;
  close cur_repolicy;

   --�ֱ������㴫���ֵӦ���Ƿֱ����źͳ�������
   --v_reply_para.certiNo     := rec_repolicy.ProposalNo;
   v_reply_para.repolicyno  :=rec_repolicy.repolicyno;
   v_reply_para.reendortimes:='000';
   v_reply_para.DangerUnitNo:= rec_repolicy.DangerUnitNo;
   v_reply_para.ReRiskCode  := rec_repolicy.ReRiskCode;
   v_reply_para.RiskCode    := rec_repolicy.RiskCode;
   v_reply_para.ComCode     := rec_repolicy.ComCode;
   v_reply_para.UwYear      := to_number(to_char(rec_repolicy.startdate,'YYYY'));
   v_reply_para.restartdate := rec_repolicy.restartdate;
   v_reply_para.startdate   := rec_repolicy.startdate;
   v_reply_para.DangerType  := rec_repolicy.DangerType;
   v_reply_para.DangerCode  := rec_repolicy.DangerCode;
   v_reply_para.CoinsInd    := rec_repolicy.CoinsInd;
   v_reply_para.BusinessInd := rec_repolicy.BusinessInd ;
   v_reply_para.BaseRate    := rec_repolicy.BaseRate ; --������������
   v_reply_para.OriCurr     := rec_repolicy.currency;
   v_reply_para.OriPML      := rec_repolicy.Pml;
   v_reply_para.CurPML      := rec_repolicy.Pml*rec_repolicy.sharerate/100; --�۳��ٷ�
   v_reply_para.RetentValue := rec_repolicy.RetentValue;
   v_reply_para.RecTimes    := rec_repolicy.rectimes;
   v_reply_para.specialind  := rec_repolicy.specialInd;
   v_reply_para.retentvaluelimit := rec_repolicy.retentvaluelimit;
   v_reply_para.combineInd := rec_repolicy.combineind;
   --add by 2013-07-11��Լ��Ҫ�����Ƿ�ũ����Y��N��
   v_reply_para.channelClass :=rec_repolicy.channel_class;
    --add by 2013-07-11��Լ������Ҫ������ҵ���
   v_reply_para.industryClass := rec_repolicy.industry_class;
   -- add by wwp
   v_reply_para.channelcode := rec_repolicy.channelcode;
   --modify by wangmx 2017728
   v_reply_para.tax := rec_repolicy.tax;
   v_reply_para.taxInd := rec_repolicy.taxind;
   if v_reply_para.RiskCode = '1811' then
     v_reply_para.startdate := rec_repolicy.issuedate;
     end if;
  --ʵ�������븽���շ����ۻ���ͬ�����ֱ�(��Ʋ�����������)
  g_errcode:='B1102';
  g_errmsg :='��ȡ�����븽���յ��ۻ�����';
  /*Get_Acc_PML(rec_repolicy.policyno, '000', rec_repolicy.RiskCode, rec_repolicy.ReRiskCode,
              rec_repolicy.DangerUnitNo, rec_repolicy.currency, v_AccOriPml, v_AccCurPml);
  if v_AccOriPml= -1 then
    v_AccOriPml := 0;
    v_AccCurPml := 0;
  else
    --ͨ��ǿ���޸�Σ�յ�λ���͵ķ�ʽ���ﵽ������BlockCode�����ۼƼ���
    v_reply_para.DangerType := 'O';
  end if;*/
  v_OriPml := v_reply_para.OriPML;
  v_CurPml := v_reply_para.CurPML;
  --add by 2013-10-16ͬ�����ֱ����ܸ���

  --�ж��Ƿ�������
  get_ContainCEM('1',v_reply_para,v_tempCem);
  if v_tempCem = 1 then

  get_RiskUnitPmlAndRet('T',rec_repolicy.proposalno,rec_repolicy.dangerunitno,rec_repolicy.riskcode,rec_repolicy.reriskcode,v_tempPml,v_tempRet);
  if v_tempPml<>0 then
      v_OriPml := v_tempPml;
      --��������ٷֱ���ȷ�����պ͸������ٷֱ�����ȫһ������Ȼ�����ܴﵽͬ�����ֱ�
      v_CurPml := v_tempPml*rec_repolicy.sharerate/100;
      v_reply_para.RetentValue:=v_tempRet;
  end if;

  end if;

  v_reply_para.OriPML := v_OriPml + v_AccOriPml;
  v_reply_para.CurPML := v_CurPml + v_AccCurPml;

  --add by liupeng 2016-11-30
  --�������죬�ж�������ڲ������ĵ����������Ŵ���������ʧ
  if rec_repolicy.incoinsind='1' then
    if rec_repolicy.coinsind = '1' then
      v_reply_para.OriPML := v_reply_para.OriPML*rec_repolicy.incoinsratesum/100;
      v_reply_para.CurPML := v_reply_para.CurPML*rec_repolicy.incoinsratesum/100;
    elsif rec_repolicy.coinsind = '2' then
      v_reply_para.OriPML := v_reply_para.OriPML*rec_repolicy.incoinsratesum/rec_repolicy.baserate;
      v_reply_para.CurPML := v_reply_para.CurPML*rec_repolicy.incoinsratesum/rec_repolicy.baserate;
    end if;
  end if;

  --������Լ�ֱ�����,���������ķֳ�����ֳ�����������v_Arr_re_Abs��
  g_errcode:='B1103';
  g_errmsg :='�ֱ����ֱ��������';
  prop_contract_cal('1',v_reply_para,v_Arr_Re_Abs);

  --���ѱ������
  v_short_rate := 1;
  /*if trunc(rec_repolicy.enddate)-trunc(rec_repolicy.restartdate)+1<=0 then
   v_short_rate:=0;
  else
   v_short_rate:=(trunc(rec_repolicy.enddate)-trunc(rec_repolicy.restartdate)+1)/(trunc(rec_repolicy.enddate)+1-trunc(rec_repolicy.startdate));
  end if;*/
  n_cnt1:=1;
  for n_cnt1 in 1..v_Arr_Re_Abs.count loop
    if v_CurPml + v_AccCurPml=0 then
       v_Arr_Re_Abs(n_cnt1).RISum:=0;
    else
       --v_Arr_Re_Abs(n_cnt1).RISum:=v_Arr_Re_Abs(n_cnt1).RISum*v_CurPml/(v_CurPml + v_AccCurPml);--���ݱ���ռ�����÷ֱ���
       --modify by liupeng 2017-1-6
       --����ֱ���PML��ֱ����㴫����ٱ�����һ�������Ӧ��Լ�ֳ������Էֱ�������Ϊ׼ �������¼���
       --������������㾫�����⣬�÷ֱ���PML*�ֳ���������������ķֳ�������ʵ������������΢С����
       if rec_repolicy.Pml<>v_reply_para.OriPML then
          v_Arr_Re_Abs(n_cnt1).RISum:=rec_repolicy.Pml*v_Arr_Re_Abs(n_cnt1).sharerate/100;
       end if;

    end if;
    v_Arr_Re_Abs(n_cnt1).grsprem:=rec_repolicy.grsprem*v_Arr_Re_Abs(n_cnt1).exchrate*v_Arr_Re_Abs(n_cnt1).sharerate*v_short_rate/100;
    v_Arr_Re_Abs(n_cnt1).netprem:=rec_repolicy.netprem*v_Arr_Re_Abs(n_cnt1).exchrate*v_Arr_Re_Abs(n_cnt1).sharerate*v_short_rate/100;
    if v_Arr_Re_Abs(n_cnt1).netInd='0' then --ë����
       v_Arr_Re_Abs(n_cnt1).Riprem:=v_Arr_Re_Abs(n_cnt1).grsprem;
    elsif v_Arr_Re_Abs(n_cnt1).netInd='1' then --������
       v_Arr_Re_Abs(n_cnt1).Riprem:=v_Arr_Re_Abs(n_cnt1).Netprem;
    else --�����򸽼�����
       v_Arr_Re_Abs(n_cnt1).Riprem:=v_Arr_Re_Abs(n_cnt1).grsprem;
    end if;
    if v_Arr_Re_Abs(n_cnt1).taxind = '1' then
      v_Arr_Re_Abs(n_cnt1).tax := v_Arr_Re_Abs(n_cnt1).Riprem * 0.06;
    else
      v_Arr_Re_Abs(n_cnt1).tax := 0;
    end if;
    --�����Լ�ֱ���Ӷ��
    v_Arr_Re_Abs(n_cnt1).RiComm:=0;
    for rec_ttySectReins in cur_ttySectReins(v_Arr_Re_Abs(n_cnt1).TtyID,v_Arr_Re_Abs(n_cnt1).sectno) loop
    -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼�� begin
      v_Ricommrateadjind:=rec_ttySectReins.Ricommrateadjind;
      begin
        open cur_PolicyPlanAdj(rec_repolicy.policyno, rec_repolicy.DangerUnitNo, rec_repolicy.ReRiskCode, rec_repolicy.RiskCode, v_Arr_Re_Abs(n_cnt1).TtyID, '000');
        fetch cur_PolicyPlanAdj into rec_PolicyPlanAdj;
        if cur_PolicyPlanAdj%notfound then
           select '*' into v_char from dual where 1=2;
        end if;
        close cur_PolicyPlanAdj;
        exception when others then
          v_Ricommrateadjind:='N';
      end;
      if rec_PolicyPlanAdj.Openind<>'N' and v_Ricommrateadjind='Y' and rec_PolicyPlanAdj.Ricommrate is not null then
        v_Arr_Re_Abs(n_cnt1).RiComm:=v_Arr_Re_Abs(n_cnt1).RiComm+v_Arr_Re_Abs(n_cnt1).Riprem*rec_ttySectReins.Sharerate/100*rec_PolicyPlanAdj.Ricommrate/100;
      else
        v_Arr_Re_Abs(n_cnt1).RiComm:=v_Arr_Re_Abs(n_cnt1).RiComm+v_Arr_Re_Abs(n_cnt1).Riprem*rec_ttySectReins.Sharerate/100*rec_ttySectReins.Rcrate/100;
      end if;
      -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼�� end
    end loop;
  end loop;

  g_errcode:='B1105';
  g_errmsg :='�ֱ����������ֱ��������';
  --����ֳ������
  crt_ply_abs(rec_repolicy.repolicyno,rec_repolicy.rectimes,v_Arr_Re_Abs);

  update REINS_REPOLICY set status='1',caldate=sysdate,date_updated=sysdate
   where repolicyno=p_repolicyno and rectimes=p_rectimes;
  commit;

  p_message_code:='0';
  exception when others then
    if v_error_Code='01' then
      p_message_code:='0';
      p_message_desc:='�˷ֱ����Ѽ���������';

   else
      p_message_code:='1';
      p_message_desc:='����ʧ��';
   end if;
   --���������־
   g_errmsg:=g_errmsg||'-'||substr(sqlerrm,1,100);
   --dbms_output.put_line('---------'||sqlerrm);
   rollback;
   if v_error_Code<>'01' then
     update REINS_REPOLICY set status='2',date_updated=sysdate
      where repolicyno=p_repolicyno and rectimes=p_rectimes;
     insert into REINS_POLICY_ERR_LOG(errtype,repolicyno,rectimes,errmsg)
            values('B1',p_repolicyno,p_rectimes,g_errmsg);
     commit;
   end if;
end quota_ply_cal;

/*quota_edr_cal���ں�Լ����������*/
procedure quota_edr_cal(p_repolicyno in REINS_REENDOR.repolicyno%type,
                        p_reendortimes in REINS_REENDOR.reendortimes%type,
                        p_RecTimes  in REINS_REENDOR.RecTimes%type,
                        p_message_code out varchar2,
                        p_message_desc out varchar2) is
cursor cur_Reendor is
  select reendor.*
   from REINS_REENDOR reendor
  where reendor.repolicyno=p_repolicyno
   and reendor.reendortimes=p_reendortimes
   and reendor.RecTimes=p_RecTimes
   and reendor.Status in ('0','2') for update; --ֻ����δ����ʹ����
  rec_Reendor cur_Reendor%rowtype;
-- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼�� begin
cursor cur_ttySectReins(p_ttyid varchar2,p_sectno varchar2) is
  select b.sharerate, a.rcrate,c.ricommrateadjind
    from REINS_TTY_SECT_REINS a, REINS_TTY_REINS b ,REINS_TTY_SECT c
   where a.ttyid = b.ttyid
     and a.ttyid = p_ttyid
     and a.sectno = p_sectno
     and a.ttyid = c.ttyid
     and a.sectno = c.sectno
     and a.reinscode = b.reinscode
     and nvl(a.brokercode,'*')=nvl(b.brokercode,'*');
  rec_ttySectReins cur_ttySectReins%rowtype;
-- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼�� end
cursor cur_endorunit(p_policyno varchar2,p_endortimes varchar2,p_dangerunitno number,p_riskcode varchar2,p_reriskcode varchar2) is
   select t.endorno certino,'E' bustype
     from reins_endor_unit t
    where t.policyno = p_policyno
      and t.endortimes = p_endortimes
      and t.dangerunitno = p_dangerunitno
      and t.riskcode = p_riskcode
      and t.reriskcode = p_reriskcode
      and t.endortimes<>'000'
   union
      select t.proposalno certino,'T' bustype
     from reins_policy_unit t
    where t.policyno = p_policyno
      and t.dangerunitno = p_dangerunitno
      and t.riskcode = p_riskcode
      and t.reriskcode = p_reriskcode
      and t.endortimes<>'000';
    rec_endorunit cur_endorunit%rowtype;
-- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼�� begin
cursor cur_PolicyPlanAdj(v_policyno Reins_Policy_Plan_Adj.Policyno%type,
                         v_dangerunitno Reins_Policy_Plan_Adj.Dangerunitno%type,
                         v_reriskcode Reins_Policy_Plan_Adj.Reriskcode%type,
                         v_riskcode Reins_Policy_Plan_Adj.Riskcode%type,
                         v_ttyid Reins_Policy_Plan_Adj.Ttyid%type,
                         v_endortimes Reins_Policy_Plan_Adj.Endortimes%type
                         ) is
  select a.openind, a.ricommrate
    from Reins_Policy_Plan_Adj a
   where a.policyno = v_policyno
     and a.dangerunitno = v_dangerunitno
     and a.reriskcode = v_reriskcode
     and a.riskcode = v_riskcode
     and a.ttyid = v_ttyid
     and a.endortimes = v_endortimes
     union
  select b.openind, b.ricommrate
    from reins_reply_plan_adj b
   where b.repolicyno = p_repolicyno
     and b.reendortimes = p_reendortimes
     and b.rectimes = p_RecTimes
     and b.ttyid = v_ttyid;
 rec_PolicyPlanAdj cur_PolicyPlanAdj%rowtype;
-- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼�� end
  v_Arr_Re_Abs      Arr_Re_Abs;  --�ֳ����
  v_last_re_abs     Arr_Re_Abs; --����ϴηֱ����
  v_reply_para      reply_para;   --�ֱ�����
  n_cnt1            number(3):=0;
  v_short_rate      REINS_REPLY_SHARE.ExchRate%type; --���ڷ��ʣ����Ƿֱ������뱣�����ڲ�һ�����
  v_unit_para       policy_unit_para;  -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼��
--  v_N_termi_rate REINS_REPLY_SHARE.ExchRate%type;--ԭ�������ڱ���

  v_char            char(1); --�����쳣��ֹ
  v_error_code      varchar(2):='00';
  v_last_reendortimes REINS_REPOLICY.reendortimes%type;
  v_Status           REINS_REPOLICY.status%type;
  v_LastGEarnedfPrem REINS_REPOLICY.grsprem%type:=0;--�ϴ�����ë����
  v_LastNEarnedfPrem REINS_REPOLICY.grsprem%type:=0;--�ϴ����ھ�����

  v_OriPml     REINS_REPOLICY.pml%type:=0;
  v_CurPml     REINS_REPOLICY.pml%type:=0;
  v_AccOriPml  REINS_REPOLICY.pml%type:=0;
  v_AccCurPml  REINS_REPOLICY.pml%type:=0;
  v_flag       varchar2(1):='0'; --PML�Ƿ�Ϊ0
  v_tempPml number(16,2):=0;
  v_tempRet number(16,2):=0;
  v_channel_class varchar2(12);
  v_industry_class varchar2(12);
  v_tempCem  number(3):=0 ; --�ж��Ƿ�������
  v_Ricommrateadjind REINS_TTY_SECT.Ricommrateadjind%type:='N';  -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼��

begin
  p_message_code:='0';
  open cur_Reendor;
   fetch cur_Reendor into rec_Reendor;
     if cur_Reendor%notfound then
      v_error_Code:='01';
      g_errcode:='B2101';
      g_errmsg :='�������Ҳ���';
      close cur_Reendor;
      select '*' into v_char from dual where 1=2;
     end if;
  close cur_Reendor;

  begin
    select channel_class, industry_class
      into v_channel_class, v_industry_class
      from reins_repolicy
     where repolicyno = rec_Reendor.Repolicyno
       and status in ('0','1','2')
       and rownum=1;
  exception when others then
      v_channel_class:='*';
      v_industry_class:='*';
  end;

/* ȡ�ϴ���Ч�ķֱ���״̬,
  ����ϴ�û�м��㣬���η�����Ҳ���ܼ��㣬
  ����Ѽ��㣬�����ϴηֱ������ڷֱ���*/

  v_last_reendortimes:=get_last_EndorTimes(rec_Reendor.repolicyno,p_reendortimes,rec_reendor.restartdate);
  if v_last_reendortimes<>'-1' then
     v_Status:=get_last_status(rec_Reendor.repolicyno,v_last_reendortimes);
     if v_Status<>'1' then
       g_errcode:='B2102';
       g_errmsg :='�������Ҳ������ż�����ȷ�ĵ�';
       select '*' into v_char from dual where 1=2;
     end if;
      g_errcode:='B2103';
      g_errmsg :='������ȡ�ϴηֱ������Ϣ';
    get_last_re(rec_Reendor.repolicyno,
                v_last_reendortimes,
                rec_Reendor.restartdate,
                v_last_re_abs);
    get_last_ply_earned(rec_Reendor.repolicyno,
                        rec_Reendor.Reendortimes,
                        rec_Reendor.restartdate,
                        v_LastGEarnedfPrem,
                        v_LastNEarnedfPrem);
    --v_LastGEarnedfPrem := 0;
    --v_LastNEarnedfPrem := 0;
  end if;

   --�������׼��
   --v_reply_para.certino     := rec_Reendor.ProposalNo;
   v_reply_para.DangerUnitNo:= rec_Reendor.DangerUnitNo;
   v_reply_para.ReRiskCode  := rec_Reendor.ReRiskCode;
   v_reply_para.RiskCode    := rec_Reendor.RiskCode;
   v_reply_para.ComCode     := rec_Reendor.ComCode;
   v_reply_para.UwYear      := to_number(to_char(rec_Reendor.startdate,'YYYY'));
   v_reply_para.restartdate := rec_Reendor.restartdate;
   v_reply_para.startdate   := rec_Reendor.Startdate;
   v_reply_para.DangerType  := rec_Reendor.DangerType;
   v_reply_para.repolicyno  := rec_Reendor.repolicyno;
   v_reply_para.reendortimes:= rec_Reendor.reendortimes;
   v_reply_para.DangerCode  := rec_Reendor.DangerCode;
   v_reply_para.CoinsInd    := rec_Reendor.CoinsInd;
   v_reply_para.BusinessInd := rec_Reendor.BusinessInd ;
   v_reply_para.BaseRate    := rec_Reendor.BaseRate ; --������������
   v_reply_para.OriCurr     := rec_Reendor.currency;
   v_reply_para.retentvaluelimit := rec_Reendor.retentvaluelimit;
   v_reply_para.combineInd := rec_Reendor.Combineind;
   --add by 2013-08-01 ��Լ��Ҫ�����Ƿ�ũ��(Y��N��)����ҵ���
   v_reply_para.channelClass := v_channel_class;
   v_reply_para.industryClass := v_industry_class;

   -- add by wwp
   v_reply_para.channelcode := rec_Reendor.Channelcode;
   v_reply_para.tax :=rec_Reendor.Tax;
   v_reply_para.taxInd := rec_Reendor.Taxind;

   if v_reply_para.RiskCode = '1811' then
     v_reply_para.startdate := rec_Reendor.issuedate;
     end if;

   --ע�����˱����⴦��
   if rec_Reendor.Pml<>0 then
      v_reply_para.OriPML      := rec_Reendor.Pml;
      v_reply_para.CurPML      := rec_Reendor.Pml*rec_Reendor.sharerate/100; --�۳��ٷ�
   else
      if rec_Reendor.ChgPml<>0 then
        v_reply_para.OriPML      := abs(rec_Reendor.ChgPml);
        v_reply_para.CurPML      := abs(rec_Reendor.ChgPml)*rec_Reendor.sharerate/100; --�۳��ٷ�
      else
        v_reply_para.OriPML      := 1;
        v_reply_para.CurPML      := 1;
      end if;
      v_flag:='1';
   end if;
   v_reply_para.RetentValue := rec_Reendor.RetentValue;
   v_reply_para.RecTimes    := rec_Reendor.Rectimes;
   v_reply_para.SpecialInd  := rec_Reendor.Specialind;

  --ʵ�������븽���շ����ۻ���ͬ�����ֱ�(��Ʋ�����������)
  g_errcode:='B2104';
  g_errmsg :='��ȡ�����븽���յ��ۻ�����';
  /*Get_Acc_PML(rec_Reendor.policyno, rec_Reendor.Endortimes, rec_Reendor.RiskCode, rec_Reendor.ReRiskCode,
              rec_Reendor.DangerUnitNo, rec_Reendor.currency, v_AccOriPml, v_AccCurPml);
  if v_AccOriPml= -1 then
    v_AccOriPml := 0;
    v_AccCurPml := 0;
  else
    --ͨ��ǿ���޸�Σ�յ�λ���͵ķ�ʽ���ﵽ������BlockCode�����ۼƼ���
    v_reply_para.DangerType := 'O';
  end if;*/
  v_OriPml := v_reply_para.OriPML;
  v_CurPml := v_reply_para.CurPML;

  open cur_endorunit(rec_Reendor.Policyno,rec_Reendor.Endortimes,rec_Reendor.Dangerunitno,rec_Reendor.Riskcode,rec_Reendor.Reriskcode);
    fetch cur_endorunit into rec_endorunit;
  close cur_endorunit;

  get_ContainCEM('1',v_reply_para,v_tempCem);
  if v_tempCem = 1 then

  --add by 2013-10-16ͬ�����ֱ����ܸ���
  get_RiskUnitPmlAndRet(rec_endorunit.bustype,rec_endorunit.certino,rec_Reendor.dangerunitno,rec_Reendor.riskcode,rec_Reendor.reriskcode,v_tempPml,v_tempRet);
  if v_tempPml<>0 then
      v_OriPml := v_tempPml;
      --��������ٷֱ���ȷ�����պ͸������ٷֱ�����ȫһ������Ȼ�����ܴﵽͬ�����ֱ�
      v_CurPml := v_tempPml*rec_Reendor.sharerate/100;
      v_reply_para.RetentValue:=v_tempRet;
  end if;

  end if;

  v_reply_para.OriPML := v_OriPml + v_AccOriPml;
  v_reply_para.CurPML := v_CurPml + v_AccCurPml;

  --add by liupeng 2016-11-30
  --�������죬�ж�������ڲ������ĵ����������Ŵ���������ʧ
  if rec_Reendor.incoinsind='1' then
    if rec_Reendor.Coinsind = '1' then
      v_reply_para.OriPML := v_reply_para.OriPML*rec_Reendor.incoinsratesum/100;
      v_reply_para.CurPML := v_reply_para.CurPML*rec_Reendor.incoinsratesum/100;
    elsif rec_reendor.coinsind = '2' then
    v_reply_para.OriPML := v_reply_para.OriPML*rec_Reendor.incoinsratesum/rec_Reendor.baserate;
    v_reply_para.CurPML := v_reply_para.CurPML*rec_Reendor.incoinsratesum/rec_Reendor.baserate;
    end if;
  end if;

  --���ñ�����Լ���㷽�����������·ֱ��������·ֱ����
  prop_contract_cal('1',v_reply_para,v_Arr_Re_Abs);
  if v_flag='1' then
    v_reply_para.OriPML := 0;
    v_reply_para.CurPML := 0;
    for n_cnt1 in 1..v_Arr_Re_Abs.count loop
      v_Arr_Re_Abs(n_cnt1).risum:=0;
    end loop;
  end if;

  --���Ѽ���
  v_short_rate := 1;
  if rec_Reendor.enddate = rec_Reendor.validdate then
    v_short_rate:= 1;
  else
    v_short_rate:=(rec_Reendor.enddate-rec_Reendor.restartdate)/(rec_Reendor.enddate-rec_Reendor.validdate);
  end if;

  for n_cnt1 in 1..v_Arr_Re_Abs.count loop
   --modify by liupeng 20160622
   --����������Լ�ķֳ��������Ҫ���ݷֳ�ռ������
   /*if v_CurPml + v_AccCurPml=0 then
      v_Arr_Re_Abs(n_cnt1).RISum:=0;
   else
      --v_Arr_Re_Abs(n_cnt1).RISum := v_Arr_Re_Abs(n_cnt1).RISum * v_CurPml/(v_CurPml + v_AccCurPml); --���ݱ���ռ�����÷ֱ���
      v_Arr_Re_Abs(n_cnt1).RISum := rec_Reendor.Pml*v_Arr_Re_Abs(n_cnt1).sharerate/100;
   end if;*/
   --modify by liupeng 2017-1-6
   --����ֱ���PML��ֱ����㴫����ٱ�����һ�������Ӧ��Լ�ֳ������Էֱ�������Ϊ׼ �������¼���
   --������������㾫�����⣬�÷ֱ���PML*�ֳ���������������ķֳ�������ʵ������������΢С����
   if rec_Reendor.Pml<>v_reply_para.OriPML then
      v_Arr_Re_Abs(n_cnt1).RISum := rec_Reendor.Pml*v_Arr_Re_Abs(n_cnt1).sharerate/100;
   end if;

     --����δ����ë����=�����ܱ���-�ϴ����ڱ���-���α仯��
     v_Arr_Re_Abs(n_cnt1).GPortfPrem:=((rec_Reendor.grsprem-v_LastGEarnedfPrem-rec_Reendor.ChgGrsPrem)+rec_Reendor.ChgGrsPrem*v_short_rate)*v_Arr_Re_Abs(n_cnt1).sharerate*v_Arr_Re_Abs(n_cnt1).exchrate /100;
     v_Arr_Re_Abs(n_cnt1).NPortfPrem:=((rec_Reendor.NetPrem-v_LastNEarnedfPrem-rec_Reendor.ChgNetPrem)+rec_Reendor.ChgNetPrem*v_short_rate)*v_Arr_Re_Abs(n_cnt1).sharerate*v_Arr_Re_Abs(n_cnt1).exchrate /100;
     v_Arr_Re_Abs(n_cnt1).ChgRISum:=rec_Reendor.ChgPml*v_Arr_Re_Abs(n_cnt1).sharerate /100;
     v_Arr_Re_Abs(n_cnt1).GEarnedPrem :=0;
     v_Arr_Re_Abs(n_cnt1).NEarnedPrem :=0;
     v_Arr_Re_Abs(n_cnt1).GrsPrem:=v_Arr_Re_Abs(n_cnt1).GEarnedPrem + v_Arr_Re_Abs(n_cnt1).GPortfPrem;
     v_Arr_Re_Abs(n_cnt1).NetPrem:=v_Arr_Re_Abs(n_cnt1).NEarnedPrem + v_Arr_Re_Abs(n_cnt1).NPortfPrem;
     v_Arr_Re_Abs(n_cnt1).ChgGrsPrem:=v_Arr_Re_Abs(n_cnt1).GrsPrem;
     v_Arr_Re_Abs(n_cnt1).ChgNetPrem:=v_Arr_Re_Abs(n_cnt1).NetPrem;
     if v_Arr_Re_Abs(n_cnt1).netInd='0' then --ë����
        v_Arr_Re_Abs(n_cnt1).ChgRIPrem:=v_Arr_Re_Abs(n_cnt1).ChgGrsPrem;
        v_Arr_Re_Abs(n_cnt1).RIPrem:=v_Arr_Re_Abs(n_cnt1).GrsPrem;
     elsif v_Arr_Re_Abs(n_cnt1).netInd='1' then --������
        v_Arr_Re_Abs(n_cnt1).ChgRIPrem:=v_Arr_Re_Abs(n_cnt1).ChgNetPrem;
        v_Arr_Re_Abs(n_cnt1).RIPrem:=v_Arr_Re_Abs(n_cnt1).NetPrem;
     end if;
     if v_Arr_Re_Abs(n_cnt1).taxInd = '1' then
       v_Arr_Re_Abs(n_cnt1).tax := v_Arr_Re_Abs(n_cnt1).RIPrem * 0.06;
       v_Arr_Re_Abs(n_cnt1).changetax := v_Arr_Re_Abs(n_cnt1).ChgRIPrem * 0.06;
     else
       v_Arr_Re_Abs(n_cnt1).tax := 0;
       v_Arr_Re_Abs(n_cnt1).changetax :=0;
     end if;
   --�����Լ�ֱ���Ӷ��
     v_Arr_Re_Abs(n_cnt1).RiComm:=0;
     v_Arr_Re_Abs(n_cnt1).ChgRiComm:=0;

     for rec_ttySectReins in cur_ttySectReins(v_Arr_Re_Abs(n_cnt1).TtyID,v_Arr_Re_Abs(n_cnt1).sectno) loop
      -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼�� begin
      v_Ricommrateadjind:=rec_ttySectReins.Ricommrateadjind;
      begin
        open cur_PolicyPlanAdj(rec_Reendor.policyno,
                               rec_Reendor.DangerUnitNo,
                               rec_Reendor.ReRiskCode,
                               rec_Reendor.RiskCode,
                               v_Arr_Re_Abs(n_cnt1).TtyID,
                               rec_Reendor.Endortimes);
        fetch cur_PolicyPlanAdj
          into rec_PolicyPlanAdj;
        if cur_PolicyPlanAdj%notfound then
          select '*' into v_char from dual where 1 = 2;
        end if;
        close cur_PolicyPlanAdj;
      exception
        when others then
          v_Ricommrateadjind := 'N';
      end;
      if rec_PolicyPlanAdj.Openind <> 'N' and v_Ricommrateadjind='Y' and rec_PolicyPlanAdj.Ricommrate is not null then
         v_Arr_Re_Abs(n_cnt1).ChgRiComm:=v_Arr_Re_Abs(n_cnt1).ChgRiComm+v_Arr_Re_Abs(n_cnt1).ChgRiprem*rec_ttySectReins.Sharerate/100*rec_PolicyPlanAdj.Ricommrate/100;
      else
         v_Arr_Re_Abs(n_cnt1).ChgRiComm:=v_Arr_Re_Abs(n_cnt1).ChgRiComm+v_Arr_Re_Abs(n_cnt1).ChgRiprem*rec_ttySectReins.Sharerate/100*rec_ttySectReins.Rcrate/100;
      end if;
      -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼�� end
     end loop;
   --v_Arr_Re_Abs(n_cnt1).RISum:=rec_Reendor.Amount*v_Arr_Re_Abs(n_cnt1).sharerate /100;
  end loop;

  --���������֮�ֱ��Ѽ��㹫ʽ
  g_errcode:='B2106';
  g_errmsg :='�������ϲ�����������';
  -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼�� begin
  v_unit_para.PolicyNo:=rec_Reendor.policyno;
  v_unit_para.DangerUnitNo:=rec_Reendor.DangerUnitNo;
  v_unit_para.ReRiskCode:=rec_Reendor.ReRiskCode;
  v_unit_para.RiskCode:=rec_Reendor.RiskCode;
  v_unit_para.Endortimes:=rec_Reendor.Endortimes;

  jion_re_rslt(v_last_re_abs,v_unit_para,v_Arr_Re_Abs);
  -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼�� end
  g_errcode:='B2107';
  g_errmsg:='����������ֱ��������';
  crt_edr_abs(rec_Reendor.repolicyno,rec_Reendor.reendortimes,rec_Reendor.rectimes,v_Arr_Re_Abs);

  update REINS_REENDOR set status='1' ,CALDATE=sysdate,date_updated=sysdate
    where repolicyno=p_repolicyno
      and reendortimes=p_reendortimes
      and rectimes=p_rectimes;
  commit;

  exception when others then
  --dbms_output.put_line('===='||sqlerrm);
   if v_error_Code='01' then
      p_message_code:='0';
      p_message_desc:='�˷������Ѽ���������';
   else
      p_message_code:='1';
      p_message_desc:='����ʧ��';
   end if;

   --���������־
   g_errmsg:=g_errmsg||'-'|| substr(sqlerrm,1,100);
   rollback;
   if v_error_Code<>'01' then
     update REINS_REENDOR set status='2', date_updated=sysdate
       where repolicyno=p_repolicyno and reendortimes=p_reendortimes and rectimes=p_rectimes;
     insert into REINS_ENDOR_ERR_LOG(errtype,repolicyno,reendortimes,rectimes,errmsg)
                        values('B2',p_repolicyno,p_reendortimes,p_rectimes,g_errmsg);
     commit;
   end if;
end quota_edr_cal;

/*quota_edr_longrisk_cal���ڳ����շ���������*/
procedure quota_edr_longrisk_cal(p_repolicyno in REINS_REENDOR.repolicyno%type,
                                p_reendortimes in REINS_REENDOR.reendortimes%type,
                                p_RecTimes  in REINS_REENDOR.RecTimes%type,
                                p_message_code out varchar2,
                                p_message_desc out varchar2) is
cursor cur_Reendor is
  select * from REINS_REENDOR
  where repolicyno=p_repolicyno and reendortimes=p_reendortimes and RecTimes=p_RecTimes
  and Status in ('0','2') for update; --ֻ����δ����ʹ����
 rec_Reendor cur_Reendor%rowtype;

  v_Arr_Re_Abs      Arr_Re_Abs;   --�ֳ����
  v_reply_para      reply_para;   --�ֱ�����
  n_cnt1            number(3):=0;
  v_short_rate      REINS_REPLY_SHARE.ExchRate%type; --���ڷ��ʣ����Ƿֱ������뱣�����ڲ�һ�����
  v_LastGEarnedfPrem REINS_REPOLICY.grsprem%type:=0;--�ϴ�����ë����
  v_LastNEarnedfPrem REINS_REPOLICY.grsprem%type:=0;--�ϴ����ھ�����
  v_char            char(1);      --�����쳣��ֹ
  v_error_code      varchar(2):='00';
  v_flag            varchar2(1):='0'; --PML�Ƿ�Ϊ0
begin
  p_message_code:='0';
  open cur_Reendor;
   fetch cur_Reendor into rec_Reendor;
     if cur_Reendor%notfound then
      v_error_Code:='01';
      g_errcode:='B2101';
      g_errmsg :='�������Ҳ���';
      close cur_Reendor;
      select '*' into v_char from dual where 1=2;
     end if;
  close cur_Reendor;

   --�������׼��
   --v_reply_para.certino     := rec_Reendor.ProposalNo;
   v_reply_para.DangerUnitNo:= rec_Reendor.DangerUnitNo;
   v_reply_para.ReRiskCode  := rec_Reendor.ReRiskCode;
   v_reply_para.RiskCode    := rec_Reendor.RiskCode;
   v_reply_para.ComCode     := rec_Reendor.ComCode;
   v_reply_para.UwYear      := to_number(to_char(rec_Reendor.startdate,'YYYY'));
   v_reply_para.restartdate := rec_Reendor.restartdate;
   v_reply_para.startdate   := rec_Reendor.Startdate;
   v_reply_para.DangerType  := rec_Reendor.DangerType;
   v_reply_para.repolicyno  := rec_Reendor.repolicyno;
   v_reply_para.reendortimes:= rec_Reendor.reendortimes;
   v_reply_para.DangerCode  := rec_Reendor.DangerCode;
   v_reply_para.CoinsInd    := rec_Reendor.CoinsInd;
   v_reply_para.BusinessInd := rec_Reendor.BusinessInd ;
   v_reply_para.BaseRate    := rec_Reendor.BaseRate ; --������������
   v_reply_para.OriCurr     := rec_Reendor.currency;
   v_reply_para.retentvaluelimit := rec_Reendor.retentvaluelimit;
   v_reply_para.combineInd := rec_Reendor.Combineind;
   --ע�����˱����⴦��
   if rec_Reendor.Pml<>0 then
      v_reply_para.OriPML      := rec_Reendor.Pml;
      v_reply_para.CurPML      := rec_Reendor.Pml*rec_Reendor.sharerate/100; --�۳��ٷ�
   else
      v_reply_para.OriPML      := abs(rec_Reendor.ChgPml);
      v_reply_para.CurPML      := abs(rec_Reendor.ChgPml)*rec_Reendor.sharerate/100; --�۳��ٷ�
      v_flag:='1';
   end if;
   v_reply_para.RetentValue := rec_Reendor.RetentValue;
   v_reply_para.RecTimes    := rec_Reendor.Rectimes;
   v_reply_para.SpecialInd  := rec_Reendor.Specialind;

  --���ñ�����Լ���㷽�����������·ֱ��������·ֱ����
  prop_contract_cal('1',v_reply_para,v_Arr_Re_Abs);
  if v_flag='1' then
    v_reply_para.OriPML := 0;
    v_reply_para.CurPML := 0;
    for n_cnt1 in 1..v_Arr_Re_Abs.count loop
      v_Arr_Re_Abs(n_cnt1).risum:=0;
    end loop;
  end if;

  --���Ѽ���
  v_short_rate := 1;
  for n_cnt1 in 1..v_Arr_Re_Abs.count loop
   --����δ����ë����=�����ܱ���-�ϴ����ڱ���-���α仯��
   v_Arr_Re_Abs(n_cnt1).GPortfPrem:=((rec_Reendor.grsprem-v_LastGEarnedfPrem-rec_Reendor.ChgGrsPrem)+rec_Reendor.ChgGrsPrem*v_short_rate)*v_Arr_Re_Abs(n_cnt1).sharerate*v_Arr_Re_Abs(n_cnt1).exchrate /100;
   v_Arr_Re_Abs(n_cnt1).NPortfPrem:=((rec_Reendor.NetPrem-v_LastNEarnedfPrem-rec_Reendor.ChgNetPrem)+rec_Reendor.ChgNetPrem*v_short_rate)*v_Arr_Re_Abs(n_cnt1).sharerate*v_Arr_Re_Abs(n_cnt1).exchrate /100;
   --v_Arr_Re_Abs(n_cnt1).ChgRISum:=rec_Reendor.ChgPml*v_Arr_Re_Abs(n_cnt1).sharerate /100;
   v_Arr_Re_Abs(n_cnt1).GEarnedPrem :=0;
   v_Arr_Re_Abs(n_cnt1).NEarnedPrem :=0;
   v_Arr_Re_Abs(n_cnt1).GrsPrem:=v_Arr_Re_Abs(n_cnt1).GEarnedPrem + v_Arr_Re_Abs(n_cnt1).GPortfPrem;
   v_Arr_Re_Abs(n_cnt1).NetPrem:=v_Arr_Re_Abs(n_cnt1).NEarnedPrem + v_Arr_Re_Abs(n_cnt1).NPortfPrem;
   --v_Arr_Re_Abs(n_cnt1).ChgGrsPrem:=v_Arr_Re_Abs(n_cnt1).GrsPrem;
   --v_Arr_Re_Abs(n_cnt1).ChgNetPrem:=v_Arr_Re_Abs(n_cnt1).NetPrem;
   if v_Arr_Re_Abs(n_cnt1).netInd='0' then --ë����
      --v_Arr_Re_Abs(n_cnt1).ChgRIPrem:=v_Arr_Re_Abs(n_cnt1).ChgGrsPrem;
      v_Arr_Re_Abs(n_cnt1).RIPrem:=v_Arr_Re_Abs(n_cnt1).GrsPrem;
   elsif v_Arr_Re_Abs(n_cnt1).netInd='1' then --������
      --v_Arr_Re_Abs(n_cnt1).ChgRIPrem:=v_Arr_Re_Abs(n_cnt1).ChgNetPrem;
      v_Arr_Re_Abs(n_cnt1).RIPrem:=v_Arr_Re_Abs(n_cnt1).NetPrem;
   end if;

   --����仯����ֱ���Ա仯�����Ա���
    v_Arr_Re_Abs(n_cnt1).ChgGrsPrem := rec_Reendor.ChgGrsPrem * v_Arr_Re_Abs(n_cnt1).sharerate/100;
    v_Arr_Re_Abs(n_cnt1).ChgNetPrem := rec_Reendor.ChgNetPrem * v_Arr_Re_Abs(n_cnt1).sharerate/100;
    v_Arr_Re_Abs(n_cnt1).chgRiSum := rec_Reendor.ChgPml * v_Arr_Re_Abs(n_cnt1).sharerate/100;
    if v_Arr_Re_Abs(n_cnt1).netInd='0' then --ë����
       v_Arr_Re_Abs(n_cnt1).ChgRIPrem := v_Arr_Re_Abs(n_cnt1).ChgGrsPrem;
    elsif v_Arr_Re_Abs(n_cnt1).netInd='1' then --������
       v_Arr_Re_Abs(n_cnt1).ChgRIPrem := v_Arr_Re_Abs(n_cnt1).ChgNetPrem;
    end if;
  end loop;

  --���������֮�ֱ��Ѽ��㹫ʽ
  g_errcode:='B2107';
  g_errmsg:='����������ֱ��������';
  crt_edr_abs(rec_Reendor.repolicyno,rec_Reendor.reendortimes,rec_Reendor.rectimes,v_Arr_Re_Abs);

  update REINS_REENDOR set status='1' ,CALDATE=sysdate,date_updated=sysdate
    where repolicyno=p_repolicyno
      and reendortimes=p_reendortimes
      and rectimes=p_rectimes;
  commit;

  exception when others then
   if v_error_Code='01' then
      p_message_code:='0';
      p_message_desc:='�˷������Ѽ���������';
   else
      p_message_code:='1';
      p_message_desc:='����ʧ��';
   end if;

   --���������־
   g_errmsg:=g_errmsg||'-'|| substr(sqlerrm,1,100);
   rollback;
   if v_error_Code<>'01' then
     update REINS_REENDOR set status='2', date_updated=sysdate
       where repolicyno=p_repolicyno and reendortimes=p_reendortimes and rectimes=p_rectimes;
     insert into REINS_ENDOR_ERR_LOG(errtype,repolicyno,reendortimes,rectimes,errmsg)
                        values('B2',p_repolicyno,p_reendortimes,p_rectimes,g_errmsg);
     commit;
   end if;
end quota_edr_longrisk_cal;

/*quota_clm_cal���ں�Լ���ⰸ̯��,��Ҫ�㷨��get_ply_share�У�
 ȡ̯�ر������������������̯�ر����Ͱ�contribution̯�صı���*/
procedure quota_clm_cal(p_reclaimno in REINS_RECLAIM.reclaimno%type,
                        p_RecTimes  in REINS_RECLAIM.RecTimes%type,
                        p_message_code out varchar2,
                        p_message_desc out varchar2) is
cursor cur_clm is select * from REINS_RECLAIM
                   where reclaimno=p_reclaimno
                     and RecTimes=p_RecTimes
                     and status in ('0','2') for update;
rec_clm cur_clm%rowtype;

v_reclm_para reclm_para;
v_share arr_danger_share;
v_share1 arr_danger_share;
v_char char(1); --�����쳣��ֹ
v_error_code varchar(2):='00';
v_mainCurr  REINS_TREATY.Maincurr%type;

n_seq number(3);
n_tmp1 number(3);
v_p_tot_sharerate REINS_REPLY_SHARE.sharerate%type:=0;
begin
 p_message_code:=0;
 open cur_clm;
   fetch cur_clm into rec_clm;
   if cur_clm%notfound then
    v_error_Code:='01';
    g_errcode:='B3101';
    g_errmsg :='�Ѿ����ⰸ�Ҳ�������������Ϣ';
    select '*' into v_char from dual where 1=2;
   end if;
 close cur_clm;

 v_reclm_para.repolicyno:=rec_clm.repolicyno;
 v_reclm_para.policyno:=rec_clm.policyno;
 v_reclm_para.reriskcode:=rec_clm.reriskcode;
 v_reclm_para.riskcode:=rec_clm.riskcode;
 v_reclm_para.dangerunitno:=rec_clm.dangerunitno;
 v_reclm_para.Uwyear:=to_number(to_char(rec_clm.startdate,'yyyy'));
 v_reclm_para.Dangertype:=rec_clm.dangertype;
 v_reclm_para.DangerCode:=rec_clm.dangerCode;
 v_reclm_para.DamageDATE:=rec_clm.Damagedate;
 v_reclm_para.currency:=rec_clm.currency;
 --����������ڴ��ڱ���ֹ�ڣ�����ڱ���ֹ��
 if rec_clm.DamageDATE>rec_clm.enddate then
    v_reclm_para.DamageDATE:=rec_clm.enddate;
 end if;
 --�����������С�ڱ������ڣ�����ڱ�������
 if rec_clm.DamageDATE<rec_clm.startDate then
    v_reclm_para.DamageDATE:=rec_clm.startDate;
 end if;

 --�������Լ��̯�ر���,�Ѱ���'P'��'C'�źô�����
 get_ply_share(v_reclm_para,v_share);
 --����һ����̯�����
  n_seq:=1;
  if v_share.count<=0 then
    g_errcode:='B3102';
    g_errmsg :='�Ѿ����ⰸ�Ҳ����ֱ�����';
    select '*' into v_char from dual where 1=2;
  end if;

  for n_seq in 1..v_share.count loop
    if v_share(n_seq).statclass in ('0','2') then --��ԼҪ���Ǻ�ͬ�涨����
      select count(*) into n_tmp1
        from REINS_TTY_CURR
         where ttyid=v_share(n_seq).ttyid
           and Currency=rec_clm.currency;
      if n_tmp1>0 then
        v_share(n_seq).RICurr:=rec_clm.currency;
      else
        select maincurr into v_maincurr from REINS_TREATY where ttyid=v_share(n_seq).ttyid;
        v_share(n_seq).RICurr:=v_maincurr;
      end if;
      v_share(n_seq).ExchRate:=get_exchrate(rec_clm.currency,v_share(n_seq).ricurr,v_reclm_para.DamageDATE);
    else
      v_share(n_seq).ricurr:= rec_clm.currency;
      v_share(n_seq).ExchRate:=1.0;
    end if;

    if v_share(n_seq).pcind='P' then
      v_share(n_seq).paidsum:=(v_share(n_seq).sharerate*rec_clm.paidsum/100)*v_share(n_seq).ExchRate;
      v_p_tot_sharerate:=v_p_tot_sharerate+v_share(n_seq).sharerate;
    else
      v_share(n_seq).paidsum:=((100-v_p_tot_sharerate)*rec_clm.Paidsum*v_share(n_seq).sharerate/100/100)*v_share(n_seq).ExchRate;
      v_share(n_seq).sharerate:=v_share(n_seq).paidsum*100/v_share(n_seq).ExchRate/rec_clm.paidsum;
    end if;
  end loop;

  --�����ٷ�
  n_Seq:=1;
  for i in 1..v_share.count loop
    if v_share(i).statclass<>'3' then
      v_share1(n_Seq):=v_share(i);
      n_seq:=n_seq+1;
    end if;
  end loop;
  --������ⰸ����������
  g_errcode:='B3103';
  g_errmsg :='�Ѿ����ⰸ�����Ѿ�̯�ؽ����Ϣ����';
  crt_clm_abs(rec_clm.reclaimno,rec_clm.rectimes,v_share1);

  update REINS_RECLAIM set status='1', CALDATE=sysdate, date_updated=sysdate
    where reclaimno=p_reclaimno
      and rectimes=p_rectimes;
  commit;

  exception when others then
   if v_error_Code='01' then
      p_message_code:='0';
      p_message_desc:='�˷��ⰸ�ѽ��м���';
   else
      p_message_code:='1';
      p_message_desc:='����ʧ��';
   end if;

    --��¼������Ϣ
    g_errmsg:=g_errmsg||'-'|| substr(sqlerrm,1,100);
    rollback;
    if v_error_Code<>'01' then
      update REINS_RECLAIM set status='2', date_updated=sysdate
        where reclaimno=p_reclaimno and rectimes=p_rectimes;
      insert into REINS_CLAIM_ERR_LOG(errtype,reclaimno,rectimes,errmsg)
                        values('B3',p_reclaimno,p_rectimes,g_errmsg);
      commit;
    end if;
end quota_clm_cal;

/*���ں�Լδ���ⰸ̯�أ��㷨ͬ���ⰸ����*/
procedure quota_OSclm_cal(p_OSReClaimNo  in REINS_OS_RECLAIM.OSReClaimNo%type,
                          p_message_code out varchar2,
                          p_message_desc out varchar2)is
cursor cur_clm is select * from REINS_OS_RECLAIM
                   where OSreclaimno=p_OSreclaimno
                     and status in ('0','2') for update;
rec_clm cur_clm%rowtype;

v_reclm_para reclm_para;
v_share arr_danger_share;
v_share1 arr_danger_share;
v_char char(1); --�����쳣��ֹ
v_error_code varchar(2):='00';
v_mainCurr  REINS_TREATY.Maincurr%type;

n_seq number(3);
n_tmp1 number(3);
v_p_tot_sharerate REINS_REPLY_SHARE.sharerate%type:=0;

begin
 p_message_code:=0;
  open cur_clm;
    fetch cur_clm into rec_clm;
    if cur_clm%notfound then
    v_error_Code:='01';
    g_errcode:='B4101';
    g_errmsg :='δ�����ⰸ�Ҳ�������������Ϣ';
    select '*' into v_char from dual where 1=2;
  end if;
  close cur_clm;

 v_reclm_para.repolicyno:=rec_clm.repolicyno;
 v_reclm_para.Uwyear:=to_number(to_char(rec_clm.startdate,'yyyy'));
 v_reclm_para.Dangertype:=rec_clm.dangertype;
 v_reclm_para.DangerCode:=rec_clm.dangerCode;
 v_reclm_para.DamageDATE:=rec_clm.Damagedate;
 v_reclm_para.currency:=rec_clm.currency;
 v_reclm_para.riskcode:= rec_clm.riskcode;
 v_reclm_para.reriskcode:=rec_clm.reRiskcode;
 v_reclm_para.policyno:=rec_clm.policyno;
 v_reclm_para.dangerunitno:=rec_clm.dangerUnitno;
 --����������ڴ��ڱ���ֹ�ڣ�����ڱ���ֹ��
 if rec_clm.DamageDATE>rec_clm.enddate then
    v_reclm_para.DamageDATE:=rec_clm.enddate;
 end if;
 --�����������С�ڱ������ڣ�����ڱ�������
 if rec_clm.DamageDATE<rec_clm.startDate then
    v_reclm_para.DamageDATE:=rec_clm.startDate;
 end if;

  g_errcode:='B4102';
  g_errmsg :='δ�����ⰸ�����Լ��̯�ر�������';
  get_ply_share(v_reclm_para,v_share);
  if v_share.count<=0 then
    g_errmsg :='δ�����ⰸ�Ҳ����ֱ�����';
    select '*' into v_char from dual where 1=2;
  end if;

  g_errcode:='B4103';
  g_errmsg :='δ�����ⰸ����δ��̯��������';
  n_seq:=1;
  for n_seq in 1..v_share.count loop
    if v_share(n_seq).statclass in('0','2') then --��ԼҪ���Ǻ�ͬ�涨����
      select maincurr into v_maincurr from REINS_TREATY where ttyid=v_share(n_seq).ttyid;
      select count(*) into n_tmp1
        from REINS_TTY_CURR
         where ttyid=v_share(n_seq).ttyid
           and Currency=rec_clm.currency;
       if n_tmp1>0 then
        v_share(n_seq).RICurr:=rec_clm.currency;
       else
        v_share(n_seq).RICurr:=v_maincurr;
       end if;
       v_share(n_seq).ExchRate:=get_exchrate(rec_clm.currency,v_share(n_seq).ricurr,v_reclm_para.DamageDATE);
    else
       v_share(n_seq).ricurr:= rec_clm.currency;
       v_share(n_seq).ExchRate:=1.0;
    end if;

   if v_share(n_seq).pcind='P' then
    v_share(n_seq).paidsum:=(v_share(n_seq).sharerate*rec_clm.OSsum/100)*v_share(n_seq).ExchRate;
    v_p_tot_sharerate:=v_p_tot_sharerate+v_share(n_seq).sharerate;
   else
    v_share(n_seq).paidsum:=((100-v_p_tot_sharerate)*rec_clm.OSsum*v_share(n_seq).sharerate/100/100)*v_share(n_seq).ExchRate;
    v_share(n_seq).sharerate:=v_share(n_seq).paidsum*100/v_share(n_seq).ExchRate/rec_clm.OSsum;
   end if;
  end loop;

  --�����ٷ�
  n_Seq:=1;
  for i in 1..v_share.count loop
   if v_share(i).statclass<>'3' then
     v_share1(n_Seq):=v_share(i);
     n_seq:=n_seq+1;
   end if;
  end loop;

  g_errcode:='B4104';
  g_errmsg :='����δ��̯�ؽ����Ϣ����';
  crt_OSclm_abs(rec_clm.OSreclaimno,v_share1);

  update REINS_OS_RECLAIM set status='1' ,CALDATE=sysdate, date_updated=sysdate
    where OSreclaimno=p_OSreclaimno ;
  commit;

  exception when others then
   if v_error_Code='01' then
      p_message_code:='0';
      p_message_desc:='��δ���ѽ��м���';
   else
      p_message_code:='1';
      p_message_desc:='����ʧ��';
   end if;
   g_errmsg:=g_errmsg||'-'||substr(sqlerrm,1,100);
    rollback;
   --���������־
   if v_error_Code<>'01' then
     update REINS_OS_RECLAIM set status = '2', date_updated = sysdate
      where OSreclaimno = p_OSreclaimno;
     insert into REINS_OS_CLAIM_ERR_LOG (errtype, osreclaimno, errmsg, errtime)
         values ('B4', p_OSReClaimNo, g_errmsg, sysdate);
     commit;
   end if;
end quota_OSclm_cal;

/*��ȡ�ۼ�ͬ�����ֱ��ı���*/
procedure Get_Acc_PML(p_PolicyNo in REINS_REPOLICY.policyno%type,
                      p_Endortimes in REINS_REPOLICY.Endortimes%type,
                      p_RiskCode in REINS_REPOLICY.riskcode%type,
                      p_ReRiskCode in REINS_REPOLICY.reriskcode%type,
                      p_DangerUnitNo in REINS_REPOLICY.dangerUnitNo%type,
                      p_currency in REINS_REPOLICY.currency%type,
                      p_OriPml out REINS_REPOLICY.pml%type,
                      p_CurPml out REINS_REPOLICY.pml%type) is
   cursor cur_relat is
        select RiskCode2 RiskCode, ReRiskCode2 ReRiskCode, DangerUnitNo2 DangerUnitNo
        from REINS_POLICY_DANGER_UNIT_RELAT
        where PolicyNo = p_PolicyNo
          and Endortimes = p_Endortimes
          and RiskCode1 = p_RiskCode
          and ReRiskCode1 = p_ReRiskCode
          and DangerUnitNo1 = p_DangerUnitNo
        union all
        select RiskCode1 RiskCode, ReRiskCode1 ReRiskCode, DangerUnitNo1 DangerUnitNo
        from REINS_POLICY_DANGER_UNIT_RELAT
        where PolicyNo = p_PolicyNo
          and Endortimes = p_Endortimes
          and RiskCode2 = p_RiskCode
          and ReRiskCode2 = p_ReRiskCode
          and DangerUnitNo2 = p_DangerUnitNo;
   rec_relat   cur_relat%rowtype;

   cursor cur_ply is
        select pml*get_exchrate(currency,p_currency,startdate) pml,
               pml*sharerate*get_exchrate(currency,p_currency,startdate)/100 curpml
        from REINS_REPOLICY
        where PolicyNo = p_PolicyNo
          and RiskCode = rec_relat.RiskCode
          and ReRiskCode = rec_relat.ReRiskCode
          and DangerUnitNo = rec_relat.DangerUnitNo
          and status in ('0','1','2')
          and p_Endortimes = '000'
        union all
        select decode(pml,0,abs(ChgPml),pml)*get_exchrate(currency,p_currency,startdate) pml,
               decode(pml,0,abs(ChgPml),pml)*sharerate*get_exchrate(currency,p_currency,startdate)/100 curpml
        from REINS_REENDOR
        where PolicyNo = p_PolicyNo
          and Endortimes = p_Endortimes
          and RiskCode = rec_relat.RiskCode
          and ReRiskCode = rec_relat.ReRiskCode
          and DangerUnitNo = rec_relat.DangerUnitNo
          and status in ('0','1','2')
          and p_Endortimes <> '000';
   rec_ply     cur_ply%rowtype;
   v_char      char(1); --�����쳣��ֹ
begin
   open cur_relat;
   fetch cur_relat into rec_relat;
     if cur_relat%notfound then
       p_OriPml := -1;
       p_CurPml := -1;
       return;
     end if;
  close cur_relat;

  open cur_ply;
   fetch cur_ply into rec_ply;
     if cur_ply%notfound then
       select '*' into v_char from dual where 1=2;
     end if;
     p_OriPml := rec_ply.pml;
     p_CurPml := rec_ply.curpml;
  close cur_ply;

end Get_Acc_PML;

/*get_ply_share ���ڱ�����Լ̯�ؼ��� ,�õ�P/C��share*/
procedure get_ply_share(p_reclm_para in reclm_para,
                        p_share out Arr_danger_share) is
v_P_share Arr_danger_share;
v_C_share Arr_danger_share;
n_seq number(3);
n_seq1 number(3);
--v_tot_share REINS_REPLY_SHARE.sharerate%type:=0;
begin
  --���㰴��P���ֳ���̯�ر���
  get_p_share(p_reclm_para,v_p_share);
   n_seq:=0;
   n_seq1:=0;
  for i in 1..v_p_share.count loop
     p_share(i).ttyid:=v_p_share(i).ttyid;
     p_share(i).statclass:=v_p_share(i).statclass;
     p_share(i).sectno:=v_p_share(i).sectno;
     p_share(i).sharerate:=v_p_share(i).sharerate;
     p_share(i).pcind:=v_p_share(i).pcind;
--     v_tot_share:=v_tot_share+v_p_share(i).sharerate;
     n_seq:=n_seq+1;
     n_seq1:=n_seq1+1;
  end loop;

   /*if p_reclm_para.dangertype in ('A','B','C') then --Σ������Ϊ�Ʋ��ա�ˮ�ա���
     get_c_share(p_reclm_para,v_c_share);

      for i in 1..v_c_share.count loop
       p_share(n_seq+i).ttyid:=v_c_share(i).ttyid;
       p_share(n_seq+i).statclass:=v_c_share(i).statclass;
       p_share(n_seq+i).sectno:=v_c_share(i).sectno;
       p_share(n_seq+i).sharerate:=v_c_share(i).sharerate;
       p_share(n_seq+i).pcind:=v_c_share(i).pcind;
--       v_tot_share:=v_tot_share+v_c_share(i).sharerate;
       n_seq1:=n_seq1+1;
      end loop;
  end if;*/

end get_ply_share;
/*ȡ�ٷֵı����ֳ�������̯�ر������㷨ͬget_ply_share,��ͬ����
 ��Լ��ֳ������ǰ���ԼID���棬�ٷ��ǰ��ٱ������˱����̯���������*/
procedure get_fac_ply_share(p_reclm_para in reclm_para,
                            p_share out Arr_danger_fac_share) is
v_P_share Arr_danger_fac_share;
v_C_share Arr_danger_fac_share;
n_seq number(3);
v_tot_p_share1 REINS_REPLY_SHARE.sharerate%type;
v_tot_p_share2 REINS_REPLY_SHARE.sharerate%type;
v_tot_p_share  REINS_REPLY_SHARE.sharerate%type;
begin
  if p_reclm_para.RepolicyNo is not null then
   get_fac_p_share(p_reclm_para,v_p_share);
  end if;
  n_seq:=0;
  for i in 1..v_p_share.count loop
       p_share(i).confertype:=v_p_share(i).confertype;
       p_share(i).conferno:=v_p_share(i).conferno;
       p_share(i).rirefno:=v_p_share(i).rirefno;
       p_share(i).brokercode:=v_p_share(i).brokercode;
       p_share(i).reinscode:=v_p_share(i).reinscode;
       p_share(i).paycode:=v_p_share(i).paycode;
       p_share(i).agentcode:=v_p_share(i).agentcode;
       p_share(i).PCInd:=v_p_share(i).PCInd;
       p_share(i).sharerate:=v_p_share(i).sharerate;
       p_share(i).insurancetype := v_p_share(i).insurancetype;
       p_share(i).Interestinsured := v_p_share(i).Interestinsured;
       p_share(i).Remarks := v_p_share(i).Remarks;
       p_share(i).Conditions := v_p_share(i).Conditions;
       p_share(i).Deductibles := v_p_share(i).Deductibles;
       n_seq:=n_seq+1;
  end loop;

  /*if p_reclm_para.dangertype in ('A','B','C') then --Σ������Ϊ�Ʋ��ա�ˮ�ա���
     get_fac_c_share(p_reclm_para,v_c_share);
     for i in 1..v_c_share.count loop
       p_share(n_seq+i).confertype:=v_c_share(i).confertype;
       p_share(n_seq+i).conferno:=v_c_share(i).conferno;
       p_share(n_seq+i).rirefno:=v_p_share(i).rirefno;
       p_share(n_seq+i).brokercode:=v_c_share(i).brokercode;
       p_share(n_seq+i).reinscode:=v_c_share(i).reinscode;
       p_share(n_seq+i).paycode:=v_c_share(i).paycode;
       p_share(n_seq+i).agentcode:=v_c_share(i).agentcode;
       p_share(n_seq+i).PCInd:=v_c_share(i).PCInd;
       p_share(n_seq+i).sharerate:=v_c_share(i).sharerate;
     end loop;
  end if;*/


   select nvl(sum(a.ShareRate),0) into v_tot_p_share1
     from REINS_REPLY_SHARE a,REINS_REPOLICY b
    where a.RepolicyNo=b.RepolicyNo
      and a.RecTimes=b.RecTimes
      and b.Status='1'
      and b.policyNo=p_reclm_para.policyno
      and b.DANGERUNITNO=p_reclm_para.DANGERUNITNO
      and b.riskcode=p_reclm_para.riskcode
      and b.reriskcode=p_reclm_para.reriskcode
      and b.restartdate<=p_reclm_para.DamageDATE
      and b.reenddate>=p_reclm_para.DamageDATE
      and a.PCInd='P';

    select nvl(sum(a.ShareRate),0) into v_tot_p_share2
     from REINS_REENDR_SHARE a,REINS_REENDOR b
    where a.RepolicyNo=b.RepolicyNo
      and a.reendortimes=b.reendortimes
      and a.RecTimes=b.RecTimes
      and b.Status='1'
      and b.policyNo=p_reclm_para.policyno
      and b.DANGERUNITNO=p_reclm_para.DANGERUNITNO
      and b.riskcode=p_reclm_para.riskcode
      and b.reriskcode=p_reclm_para.reriskcode
      and b.restartdate<=p_reclm_para.DamageDATE
      and b.reenddate>=p_reclm_para.DamageDATE
      and a.PCInd='P' ;

    v_tot_p_share:=v_tot_p_share1 + v_tot_p_share2;

  for n_seq in 1..p_share.count loop
   if p_share(n_seq).pcind='P' then
    p_share(n_seq).paidsum:=(p_share(n_seq).sharerate*p_reclm_para.paidsum/100);
   else
    p_share(n_seq).paidsum:=(p_share(n_seq).sharerate*(100-v_tot_p_share)*p_reclm_para.paidsum/100/100);
    p_share(n_seq).sharerate:=p_share(n_seq).paidsum*100/p_reclm_para.paidsum;
   end if;
  end loop;

end get_fac_ply_share;

/* ����Ǳ����ٷֵ��ⰸ��̯��� */
procedure get_osnfac_ply_share(p_reclm_para in reclm_para,
                           p_share out Arr_danger_nfac_share,
                           p_xlsharerate out REINS_REPOLICY.sharerate%type) is
 cursor cur_lay_share is
   select distinct a.LAYERNO, a.RICURR, a.EXCHRATE,a.EXCESSLOSS, a.CONTQUOTA, a.CURCONTQUOTA,
          b.SHARERATE, a.PREMIUM, a.resmrate, a.endloss
     from REINS_REPLY_N_FAC a, REINS_REPOLICY b
    where b.policyNo=p_reclm_para.policyno
      and b.DANGERUNITNO=p_reclm_para.DANGERUNITNO
      and b.riskcode=p_reclm_para.riskcode
      and b.reriskcode=p_reclm_para.reriskcode
      and b.restartdate<=p_reclm_para.DamageDATE
      and b.reenddate>=p_reclm_para.DamageDATE
      and b.Status='1'
      and b.reinstype='2'
      and a.RepolicyNo=b.RepolicyNo
      and a.RecTimes=b.RecTimes
  union all
   select distinct a.LAYERNO, a.RICURR, a.EXCHRATE, a.EXCESSLOSS, a.CONTQUOTA, a.CURCONTQUOTA,
          b.SHARERATE, a.PREMIUM, a.resmrate, a.endloss
     from REINS_REENDR_N_FAC a, REINS_REENDOR b
    where b.policyNo=p_reclm_para.policyno
      and b.DANGERUNITNO=p_reclm_para.DANGERUNITNO
      and b.riskcode=p_reclm_para.riskcode
      and b.reriskcode=p_reclm_para.reriskcode
      and b.restartdate<=p_reclm_para.DamageDATE
      and b.reenddate>=p_reclm_para.DamageDATE
      and b.Status='1'
      and b.reinstype='2'
      and a.RepolicyNo=b.RepolicyNo
      and a.RecTimes=b.RecTimes
      and a.reendortimes=b.reendortimes;

 rec_lay_share  cur_lay_share%rowtype;
 n_seq             number(3):=0;
 v_exchrate        number:=0;
 v_TotalPaidSum    number:=0;
 v_re_ossum      number:=0;
 v_re_paidsum    number:=0;
 v_OldPaidSum      number:=0;
 v_OldSharePaidSum number:=0;
 v_ShareOsSum      number:=0;
 v_LayerPaid       number:=0;
begin
  for rec_lay_share in cur_lay_share loop
    --modify by liupeng �Ǳ����ٷֲ����ǻָ�����
    --�����Σ�յ�λ�ۼ��������̯�����
    --�������Ѿ�
    select nvl(sum(b.paidsum *
                   get_exchrate(b.ricurr,
                                rec_lay_share.RICURR,
                                p_reclm_para.DamageDATE)),
               0)
      into v_re_paidsum
      from REINS_reCLAIM a, reins_reclaim_share b
     where a.reclaimno = b.reclaimno
       and a.rectimes = b.rectimes
       and a.CLAIMNO = p_reclm_para.claimno
       and a.DANGERUNITNO = p_reclm_para.DANGERUNITNO
       and a.RERISKCODE = p_reclm_para.RERISKCODE
       and a.RISKCODE = p_reclm_para.RISKCODE
       and a.status = '1'
       and a.reinstype = '0'
       and b.ttytype in ('81', '82');
    --������δ��
    select nvl(sum(b.ossum *
                   get_exchrate(b.ricurr,
                                rec_lay_share.RICURR,
                                p_reclm_para.DamageDATE)),
               0)
      into v_re_ossum
      from reins_os_reclaim a, reins_os_reclaim_share b
     where a.osreclaimno = b.osreclaimno
       and a.CLAIMNO = p_reclm_para.claimno
       and a.DANGERUNITNO = p_reclm_para.DANGERUNITNO
       and a.RERISKCODE = p_reclm_para.RERISKCODE
       and a.RISKCODE = p_reclm_para.RISKCODE
       and a.status = '1'
       and a.reinstype ='0'
       and b.ttytype in ('81', '82');

     /*
    -- ��������δ�����
    select nvl(sum(b.ossum * get_exchrate(currency, rec_lay_share.RICURR, p_reclm_para.DamageDATE)),0)
     into  v_ShareOsSum
      from REINS_OS_RECLAIM a, REINS_OS_RECLAIM_SHARE b
     where a.CLAIMNO = p_reclm_para.claimno
      and  a.DANGERUNITNO = p_reclm_para.DANGERUNITNO
      and  a.RERISKCODE = p_reclm_para.RERISKCODE
      and  a.RISKCODE = p_reclm_para.RISKCODE
      and  a.osreclaimno = b.osreclaimno;
      and  b.ttytype in ('81','82');
      */
    -- ��������δ�����
    /*select a.ossum* get_exchrate(a.currency, rec_lay_share.RICURR, p_reclm_para.DamageDATE)
     into  v_ShareOsSum
      from REINS_OS_RECLAIM a
     where a.CLAIMNO = p_reclm_para.claimno
      and  a.DANGERUNITNO = p_reclm_para.DANGERUNITNO
      and  a.RERISKCODE = p_reclm_para.RERISKCODE
      and  a.RISKCODE = p_reclm_para.RISKCODE
      and  a.reinstype = '2';*/
   begin
     select nvl(sum(b.chgpaidsum),0)
       into v_LayerPaid
       from REINS_RECLM_N_FAC b, REINS_RECLAIM a
      where a.reclaimno = b.reclaimno
        and a.rectimes = b.rectimes
        and a.CLAIMNO = p_reclm_para.claimno
        and a.DANGERUNITNO = p_reclm_para.DANGERUNITNO
        and a.RERISKCODE = p_reclm_para.RERISKCODE
        and a.RISKCODE = p_reclm_para.RISKCODE
        and b.layerno = rec_lay_share.layerno;
   exception when others then
     v_LayerPaid:=0;
   end;
   v_exchrate:=get_exchrate(p_reclm_para.currency, rec_lay_share.RICURR, p_reclm_para.DamageDATE);
   v_TotalPaidSum := v_re_paidsum+v_re_ossum;

    if v_TotalPaidSum>rec_lay_share.EXCESSLOSS AND rec_lay_share.endloss>0 then
       n_seq := n_seq + 1;
       p_share(n_seq).layerno := rec_lay_share.layerno;
       p_share(n_seq).paidsum := (least(v_TotalPaidSum, rec_lay_share.endloss)-rec_lay_share.EXCESSLOSS);
       p_share(n_seq).paidsum := p_share(n_seq).paidsum-v_LayerPaid;

       /*if p_share(n_seq).paidsum>v_ShareOsSum*rec_lay_share.sharerate/100 then
         p_share(n_seq).paidsum:=v_ShareOsSum*rec_lay_share.sharerate/100;
       end if;*/
       p_share(n_seq).chgpaidsum := p_share(n_seq).paidsum - v_OldSharePaidSum;
       p_share(n_seq).renprem := rec_lay_share.premium * rec_lay_share.resmrate * p_share(n_seq).paidsum / (rec_lay_share.CONTQUOTA-rec_lay_share.EXCESSLOSS);
       p_share(n_seq).chgrenprem := p_share(n_seq).renprem;
       p_share(n_seq).exchrate := v_exchrate;
    end if;

    p_xlsharerate := rec_lay_share.sharerate;
  end loop;

end get_osnfac_ply_share;

/* ����Ǳ����ٷֵ��ⰸ��̯��� */
procedure get_nfac_ply_share(p_reclm_para in reclm_para,
                           p_share out Arr_danger_nfac_share,
                           p_xlsharerate out REINS_REPOLICY.sharerate%type) is
 cursor cur_lay_share is
   select distinct a.repolicyno, a.LAYERNO, a.RICURR, a.EXCHRATE,a.EXCESSLOSS, a.CONTQUOTA, a.CURCONTQUOTA,
          b.SHARERATE, a.PREMIUM, a.resmrate, a.endloss
     from REINS_REPLY_N_FAC a, REINS_REPOLICY b
    where b.policyNo=p_reclm_para.policyno
      and b.DANGERUNITNO=p_reclm_para.DANGERUNITNO
      and b.riskcode=p_reclm_para.riskcode
      and b.reriskcode=p_reclm_para.reriskcode
      and b.restartdate<=p_reclm_para.DamageDATE
      and b.reenddate>=p_reclm_para.DamageDATE
      and b.Status='1'
      and b.reinstype='2'
      and a.RepolicyNo=b.RepolicyNo
      and a.RecTimes=b.RecTimes
  union all
   select distinct a.repolicyno, a.LAYERNO, a.RICURR, a.EXCHRATE, a.EXCESSLOSS, a.CONTQUOTA, a.CURCONTQUOTA,
          b.SHARERATE, a.PREMIUM, a.resmrate, a.endloss
     from REINS_REENDR_N_FAC a, REINS_REENDOR b
    where b.policyNo=p_reclm_para.policyno
      and b.DANGERUNITNO=p_reclm_para.DANGERUNITNO
      and b.riskcode=p_reclm_para.riskcode
      and b.reriskcode=p_reclm_para.reriskcode
      and b.restartdate<=p_reclm_para.DamageDATE
      and b.reenddate>=p_reclm_para.DamageDATE
      and b.Status='1'
      and b.reinstype='2'
      and a.RepolicyNo=b.RepolicyNo
      and a.RecTimes=b.RecTimes
      and a.reendortimes=b.reendortimes
   order by layerno;

 rec_lay_share  cur_lay_share%rowtype;
 n_seq             number(3):=0;
 v_exchrate        number:=0;
 v_TotalPaidSum    number:=0;
 v_OldPaidSum      number:=0;
 v_OldSharePaidSum number:=0;
 v_OldRenPrem      number:=0;
begin
  for rec_lay_share in cur_lay_share loop
    --modify by liupeng �Ǳ����ٷֲ����ǻָ�����
    --�����Σ�յ�λ�ۼ��������̯�����
    select nvl(sum(b.paidsum *
                   get_exchrate(b.ricurr,
                                rec_lay_share.RICURR,
                                p_reclm_para.DamageDATE)),
               0)
      into v_TotalPaidSum
      from REINS_reCLAIM a, reins_reclaim_share b
     where a.reclaimno = b.reclaimno
       and a.rectimes = b.rectimes
       and a.CLAIMNO = p_reclm_para.claimno
       and a.DANGERUNITNO = p_reclm_para.DANGERUNITNO
       and a.RERISKCODE = p_reclm_para.RERISKCODE
       and a.RISKCODE = p_reclm_para.RISKCODE
       and a.status = '1'
       and b.ttytype in ('81', '82');
    /*select nvl(sum((a.sumpaid+a.sumfee) * get_exchrate(a.currency, rec_lay_share.RICURR, p_reclm_para.DamageDATE)),0)
     into  v_OldPaidSum
      from REINS_CLAIM_UNIT a
     where a.CLAIMNO = p_reclm_para.claimno
      and  a.DANGERUNITNO = p_reclm_para.DANGERUNITNO
      and  a.RERISKCODE = p_reclm_para.RERISKCODE
      and  a.RISKCODE = p_reclm_para.RISKCODE;*/
      --and b.ttytype in ('81','82');
    select nvl(sum(c.PAIDSUM * get_exchrate(a.currency, c.currency, p_reclm_para.DamageDATE)),0)
     into  v_OldSharePaidSum
      from REINS_RECLAIM a, REINS_RECLAIM_SHARE b, REINS_RECLM_N_FAC c
     where a.CLAIMNO = p_reclm_para.claimno
      and  a.DANGERUNITNO = p_reclm_para.DANGERUNITNO
      and  a.RERISKCODE = p_reclm_para.RERISKCODE
      and  a.RISKCODE = p_reclm_para.RISKCODE
      and  a.STATUS = '1'
      and  a.RECLAIMNO = b.RECLAIMNO
      and  a.RECTIMES = b.RECTIMES
      and a.reclaimno=c.reclaimno
      and a.rectimes=c.rectimes
      and c.layerno=rec_lay_share.layerno
      and b.ttytype = '92';
    --modify by liupeng �Ǳ����ٷֲ����ǻָ�����
    /*select nvl(sum(c.renprem * get_exchrate(a.currency, c.currency, p_reclm_para.DamageDATE)),0)
     into  v_OldRenPrem
      from REINS_RECLAIM a, REINS_RECLAIM_SHARE b, REINS_RECLM_N_FAC c
     where a.CLAIMNO = p_reclm_para.claimno
      and  a.DANGERUNITNO = p_reclm_para.DANGERUNITNO
      and  a.RERISKCODE = p_reclm_para.RERISKCODE
      and  a.RISKCODE = p_reclm_para.RISKCODE
      and  a.STATUS = '1'
      and  a.RECLAIMNO = b.RECLAIMNO
      and  a.RECTIMES = b.RECTIMES
      and a.reclaimno=c.reclaimno
      and a.rectimes=c.rectimes
      and c.layerno=rec_lay_share.layerno
      and b.ttytype = '92';*/

    v_exchrate := get_exchrate(p_reclm_para.currency, rec_lay_share.RICURR, p_reclm_para.DamageDATE);
    if v_TotalPaidSum>rec_lay_share.EXCESSLOSS AND rec_lay_share.endloss>0 then
       n_seq := n_seq + 1;
       p_share(n_seq).layerno := rec_lay_share.layerno;
       p_share(n_seq).paidsum := (least(v_TotalPaidSum, rec_lay_share.endloss)-rec_lay_share.EXCESSLOSS);
       --modify by liupeng
       /*if p_share(n_seq).paidsum>p_reclm_para.paidsum*rec_lay_share.sharerate/100 then
         p_share(n_seq).paidsum:=p_reclm_para.paidsum*rec_lay_share.sharerate/100;
       end if;*/
       p_share(n_seq).chgpaidsum := p_share(n_seq).paidsum - v_OldSharePaidSum;
       --modify by liupeng �Ǳ����ٷֲ����ǻָ�����
       /*p_share(n_seq).renprem := rec_lay_share.premium * rec_lay_share.resmrate * p_share(n_seq).paidsum / (rec_lay_share.CONTQUOTA-rec_lay_share.EXCESSLOSS);
       p_share(n_seq).chgrenprem := p_share(n_seq).renprem-v_OldRenPrem;*/
       p_share(n_seq).exchrate := v_exchrate;
    end if;

    p_xlsharerate := rec_lay_share.sharerate;
  end loop;

end get_nfac_ply_share;

/* �ٷֱ������� */
procedure fac_ply_cal(p_repolicyno in REINS_REPOLICY.repolicyno%type,
                      p_RecTimes   in REINS_REPOLICY.RecTimes%type,
                      p_message_code out varchar2,
                      p_message_desc out varchar2 )    is

v_oriGrsprem REINS_REPOLICY.Pml%type;
cursor cur_tty(p_ply_year number) is
  select a.ttyid,b.StatClass,b.TtyType,a.ttycode,a.Uwyear
    from REINS_TREATY a,REINS_TTY_TABLE b
   where a.Uwyear=p_ply_year
     and a.ttycode=b.ttycode and b.ttycode = 'FAC01'
     and b.ttytype='91'
     ORDER BY a.ttyid;  --�ٷֺ�ԼID
cursor cur_ply is
  select * from REINS_REPOLICY
   where repolicyno=p_repolicyno
     and RecTimes=p_Rectimes
     and Status in ('0','2') for update;
rec_ply cur_ply%rowtype;

cursor cur_fac is
   select * from REINS_REPLY_FAC
   where repolicyno=p_repolicyno
     and RecTimes=p_Rectimes;
rec_fac cur_fac%rowType;

cursor cur_plan(v_INSTMARK REINS_REPOLICY.INSTMARK%type )is
     select 1 PayNo,rec_ply.startdate PlanDate,1 planrate
       from dual
      where v_INSTMARK='N'
    union
     select payno,PlanDate,GRSPLANFEE/v_oriGrsprem  planrate
       from REINS_POLICY_UNIT_PLAN
      where v_INSTMARK='Y'
        and PolicyNo=rec_ply.PolicyNo
        and DangerUnitNo=rec_ply.DangerUnitNo
        and ReRiskCode=rec_ply.ReRiskCode
        and RiskCode=rec_ply.RiskCode;
cursor cur_sumPlan is
    select reinscode,brokercode,confertype,sum(planfee) as planfee
    from REINS_REPLY_FAC_PLAN
    where repolicyno=p_repolicyno
     and rectimes = p_Rectimes
     group by reinscode,brokercode,confertype;
rec_sumPlan cur_sumPlan%rowtype;
rec_plan cur_plan%rowtype;
v_arr_re_abs arr_re_abs;
v_short_rate REINS_REPLY_SHARE.ExchRate%type; --���ڷ��ʣ����Ƿֱ������뱣�����ڲ�һ�����
v_char char(1); --�����쳣��ֹ
v_error_code varchar(2):='00';
v_RINetPrem REINS_REPLY_SHARE.Riprem%type;--�ٱ����ֳ�����
begin
  open cur_ply;
    fetch cur_ply into rec_ply;
       if cur_ply%notfound then
         v_error_Code:='01';
         close cur_ply;
         g_errcode:='B1201';
         g_errmsg:='�ٷֱַ��������Ҳ����ֱ���';
         select '*' into v_char from dual where 1=2;
       end if;
  close cur_ply;

  --ȡΣ�յ�λë����
  g_errmsg:='ȡΣ�յ�λë����';
  select GrsPrem into v_oriGrsprem
    from REINS_POLICY_UNIT
   where PolicyNo=rec_ply.PolicyNo
     and DangerUnitNo=rec_ply.DangerUnitNo
     and ReRiskCode=rec_ply.ReRiskCode
     and RiskCode=rec_ply.RiskCode;

   open cur_tty(to_number(to_char(rec_ply.startdate,'yyyy')));
   --v_arr_re_abs���������Ϊ�˹��ò���ֱ������ķ���
   fetch cur_tty into
      v_arr_re_abs(1).ttyid,v_arr_re_abs(1).StatClass,
      v_arr_re_abs(1).TtyType,v_arr_re_abs(1).ttycode,v_arr_re_abs(1).Uwyear;
   close cur_tty;

   --���ڷ���
   v_short_rate := 1;
   --v_short_rate:=(trunc(rec_ply.enddate)-trunc(rec_ply.restartdate)+1)/(trunc(rec_ply.enddate)+1-trunc(rec_ply.startdate));

   update REINS_REPLY_FAC set RISum=rec_ply.pml*ExchRate*sharerate/100,
                         GrsPrem=RISum*RIRate*ShortRateMole/(100*ShortRateDeno),date_updated = sysdate
      where repolicyno=p_repolicyno and RecTimes=p_Rectimes
      and RIRate<>0;

   update REINS_REPLY_FAC set NetPrem=grsPrem*rec_ply.netprem/decode(rec_ply.grsPrem,0,1,rec_ply.grsPrem),date_updated = sysdate
    where repolicyno=p_repolicyno and RecTimes=p_Rectimes
    and RIRate<>0;


   update REINS_REPLY_FAC set RISum=rec_ply.pml*ExchRate*sharerate/100,
                         GrsPrem=rec_ply.grsprem*ExchRate*sharerate*v_short_rate/100,
                         netprem=rec_ply.netprem*ExchRate*sharerate*v_short_rate/100,date_updated = sysdate
      where repolicyno=p_repolicyno
      and RecTimes=p_Rectimes
      and RIRate=0;

   /*--modify by liupeng 20170223
   --���Ӽ����ٷֳַ�������ֵ˰�߼�
   --��ֵ˰ͳһת��Ϊ�����
   update reins_reply_fac t
      set t.tax = t.grsprem *
                  get_exchrate(t.ricurr, 'CNY', rec_ply.startdate) *
                  t.taxrate / 100
    where repolicyno = p_repolicyno
      and RecTimes = p_Rectimes;*/

   --modify by liupeng 20161011
   --���mantis 0004576
   update REINS_REPLY_FAC
      set RIPrem       = decode(NetInd,
                                '0',
                                GrsPrem,
                                '1',
                                NetPRem,
                                GrsPrem),
          date_updated = sysdate
    where repolicyno = p_repolicyno
      and RecTimes = p_Rectimes;
   --modify by liupeng 20170223
   --ԭ���ٱ�������=�ֳ�����*(�����ѱ���+��˰����+�������ñ���)
   --�޸ĺ��ٱ�������=�ֳ�����*(�����ѱ���+�������ñ���)
   update REINS_REPLY_FAC
      set RIcomm       = RIPrem * (CommRate + othRate) / 100,
          tax          = RIPrem *
                         /*get_exchrate(ricurr, 'CNY', rec_ply.startdate) **/
                         taxrate / 100,
          date_updated = sysdate
    where repolicyno = p_repolicyno
      and RecTimes = p_Rectimes;
   --modify by liupeng 20170223
   --�����������ֵ˰����ֵ˰���Ͳ�ΪӦ˰������ֵ˰Ϊ0
   update reins_reply_fac t
      set t.tax = 0
    where t.repolicyno = p_repolicyno
      and t.RecTimes = p_Rectimes
      and (t.taxind <> '1' or t.payind = 'N');

   g_errcode:='B1203';
   g_errmsg:='��ȡ�ٷֱַ����(REINS_REPLY_FAC)';
   --select sum(risum/exchrate),sum(grsprem/exchrate),sum(netprem/exchrate),sum(grsprem/exchrate) into
   select sum(risum/exchrate),sum(grsprem/exchrate),sum(netprem/exchrate),sum(riprem/exchrate),sum(ricomm/exchrate) into
            v_arr_re_abs(1).RISum,v_arr_re_abs(1).GrsPrem,v_arr_re_abs(1).NetPrem,v_arr_re_abs(1).RIPrem,v_arr_re_abs(1).ricomm
      from REINS_REPLY_FAC
     where repolicyno=p_repolicyno
       and RecTimes=p_RecTimes;

  v_arr_re_abs(1).SectNo    :='0';
  v_arr_re_abs(1).ShareRate :=rec_ply.sharerate;
  v_arr_re_abs(1).RICurr    :=rec_ply.currency;
  v_arr_re_abs(1).ExchRate  :=1;
  -- v_arr_re_abs(1).NetInd:='0';
  -- v_arr_re_abs(1).PCInd:='P';
  select pcind,NetInd into v_arr_re_abs(1).PCind,v_arr_re_abs(1).NetInd
   from REINS_REPLY_FAC where repolicyno=p_repolicyno
      and RecTimes=p_RecTimes and rownum=1;

   g_errcode:='B1204';
   g_errmsg:='��REINS_REPLY_SHARE������ٷֱַ��������';
   crt_ply_abs(p_repolicyno,p_RecTimes,v_Arr_Re_Abs);

   open cur_plan(rec_ply.INSTMARK);
   fetch cur_plan into rec_plan;
     if cur_plan%notfound then
       g_errmsg:='����û�з�����Ϣ';
       close cur_plan;
       select '*' into v_char from dual where 1=2;
     end if;
   close cur_plan;

   g_errcode:='B1205';
   g_errmsg:='�����ٷֽɷѼƻ�(REINS_REPLY_FAC_PLAN)����';
   for rec_plan in cur_plan(rec_ply.INSTMARK) loop
   for rec_fac in cur_fac loop
    insert into REINS_REPLY_FAC_PLAN
      (REPOLICYNO,
       PAYNO,
       RECTIMES,
       CONFERTYPE,
       BROKERCODE,
       REINSCODE,
       PAYDATE,
       CURRENCY,
       PLANFEE,
       REMARKS)
      select rec_fac.REPOLICYNO,
             rec_plan.PAYNO,
             rec_fac.RECTIMES,
             rec_fac.ConferType,
             rec_fac.BROKERCODE,
             rec_fac.ReinsCode,
             rec_plan.plandate,
             rec_fac.Ricurr,
             --modiby by liupeng �ٱ�Ӫ���� �ٱ��ֱ��������㷨����
             --���Ϊ�Ǵ��۴��� ������=�ֳ�����+��ֵ˰ - ������
             --���Ϊ���۴���  ������=�ֳ����� - ������ - ����˰ ������˰=��ֵ˰*12%��
             (rec_fac.RIPrem - rec_fac.RIComm +
             decode(rec_fac.collectind, 'Y', 0, nvl(rec_fac.tax, 0)) -
             decode(rec_fac.collectind, 'Y', nvl(rec_fac.tax, 0) * 0.12, 0)) *
             rec_plan.planrate,
             ''
        from dual;
   end loop;
  end loop;
  --���ڸ������
  for rec_sumPlan in cur_sumPlan Loop
      select riprem - ricomm +
             decode(collectind, 'Y', 0, nvl(tax, 0)) -
             decode(collectind, 'Y', nvl(tax, 0) * 0.12, 0)
        into v_RINetPrem
        from REINS_REPLY_FAC
       where repolicyno = p_repolicyno
         and rectimes = p_rectimes
         and reinscode = rec_sumPlan.reinscode
         and confertype = rec_sumplan.confertype
         and nvl(brokercode, '*') = nvl(rec_sumPlan.brokerCode, '*');
    update REINS_REPLY_FAC_PLAN
    set planfee = planfee+v_riNetPrem - rec_sumPlan.planfee
    where repolicyno=p_repolicyno
        and rectimes = p_rectimes
        and reinscode = rec_sumPlan.reinscode
        and nvl(brokercode,'*')=nvl(rec_sumPlan.brokerCode,'*')
        and confertype=rec_sumplan.confertype
        and rownum=1;
   end loop;

  p_message_code :='0';
  p_message_desc:='����ɹ�';
  update REINS_REPOLICY set status='1',caldate=sysdate,date_updated=sysdate
   where repolicyno=p_repolicyno and rectimes=p_rectimes;
  commit;

  exception when others then
   if v_error_Code='01' then
      p_message_code:='0';
      p_message_desc:='�˷ֱ����Ѽ���������';
   else
      p_message_code:='1';
      p_message_desc:='����ʧ��';
   end if;
   --���������־
   g_errmsg:=g_errmsg||'-'||substr(sqlerrm,1,100);
   rollback;
   if v_error_Code<>'01' then
     update REINS_REPOLICY set status='2',date_updated=sysdate
      where repolicyno=p_repolicyno and rectimes=p_rectimes;
     insert into REINS_POLICY_ERR_LOG(errtype,repolicyno,rectimes,errmsg)
            values('B1',p_repolicyno,p_rectimes,g_errmsg);
     commit;
   end if;
end fac_ply_cal;

/* �ٷַ��������� */
procedure fac_edr_cal(p_repolicyno in REINS_REENDOR.repolicyno%type,
                      p_reendortimes in REINS_REENDOR.reendortimes%type,
                      p_RecTimes   in REINS_REENDOR.RecTimes%type,
                      p_message_code out varchar2,
                      p_message_desc out varchar2 ) is

--ȡ�ٷֵĺ�ԼID��Ϊ�˲���ֱ������
cursor cur_tty(p_ply_year number) is
  select a.ttyid,b.StatClass,b.TtyType,a.ttycode,a.Uwyear
    from REINS_TREATY a,REINS_TTY_TABLE b
   where a.Uwyear=p_ply_year
     and a.ttycode=b.ttycode and b.ttycode = 'FAC01'
     and b.ttytype='91'
     ORDER BY a.ttyid;  --�ٷֺ�ԼID

cursor cur_edr is
  select * from REINS_REENDOR
   where repolicyno=p_repolicyno
     and reendortimes=p_reendortimes
     and RecTimes=p_Rectimes
     and Status in ('0','2') for update;
rec_edr cur_edr%rowtype;

cursor cur_edr_share is
 select * from REINS_REENDR_FAC
  where repolicyno=p_repolicyno
    and reendortimes=p_reendortimes
    and RecTimes=p_Rectimes;
rec_edr_share cur_edr_share%rowtype;

   type fac_share is record (
    ConferType     REINS_REENDR_FAC.ConferType%type    ,
    BrokerCode     REINS_REENDR_FAC.BrokerCode%type  ,
    ReinsCode      REINS_REENDR_FAC.ReinsCode%type   ,
    RICurr         REINS_REENDR_FAC.RiCurr%type   ,
    ExchRate       REINS_REENDR_FAC.ExchRate%type ,
    NetInd         REINS_REENDR_FAC.NetInd%type      ,
    ShareRate      REINS_REENDR_FAC.ShareRate%type,
    GrsPrem        REINS_REENDR_FAC.GrsPrem%type,
    NetPrem        REINS_REENDR_FAC.NetPrem%type,
    RiPrem         REINS_REENDR_FAC.RiPrem%type,
    GEarnedPrem    REINS_REENDR_FAC.GEarnedPrem%type,
    GPortfPrem     REINS_REENDR_FAC.GPortfPrem%type,
    NEarnedPrem    REINS_REENDR_FAC.NEarnedPrem%type,
    NPortfPrem     REINS_REENDR_FAC.NPortfPrem%type,
    ChgGrsPrem     REINS_REENDR_FAC.ChgGrsPrem%type,
    ChgNetPrem     REINS_REENDR_FAC.ChgNetPrem%type,
    ChgRIPrem      REINS_REENDR_FAC.ChgRIPrem%type,
    ChgRISum       REINS_REENDR_FAC.ChgRISum%type,
    RiRate         REINS_REENDR_FAC.rirate%type,
    INSURANCETYPE  REINS_REENDR_FAC.Insurancetype%type,
    INTERESTINSURED  REINS_REENDR_FAC.Interestinsured%type,
    DEDUCTIBLES    REINS_REENDR_FAC.Deductibles%type,
    CONDITIONS     REINS_REENDR_FAC.Conditions%type,
    remarks        REINS_REENDR_FAC.Remarks%type,
    RiSum          REINS_REENDR_FAC.Risum%type
    );
   Type Arr_fac_share is table of fac_share Index By BINARY_INTEGER ;   --���ÿ�ηֱ��������
   v_last_arr_fac_share arr_fac_share;
   v_arr_fac_share arr_fac_share;

--ȡ�ϴηֱ������Ϊ�˼������ڷֱ���
cursor cur_last_re(p_Reendortimes varchar2)is
     select a.ConferType,a.BrokerCode,a.ReinsCode,a.RICurr,a.ExchRate,a.netind,
            a.ShareRate,a.GrsPrem,a.NetPrem,b.restartdate,b.reenddate,b.enddate,
            a.RIPrem,0 GEarnedPrem,0 NEarnedPrem,a.GrsPrem GPortfPrem, a.NetPrem NPortfPrem, a.risum,
            a.INSURANCETYPE,a.INTERESTINSURED,a.DEDUCTIBLES,a.CONDITIONS,a.remarks
     from REINS_REPLY_FAC a,REINS_REPOLICY b
    where a.RepolicyNo=b.RepolicyNo
      and a.RecTimes=b.RecTimes
      and b.Status='1'
      and b.RepolicyNo=rec_edr.repolicyno
      and p_Reendortimes='000'
  union
    select a.ConferType,a.BrokerCode,a.ReinsCode,a.RICurr,a.ExchRate,a.netind,
           a.ShareRate,a.GrsPrem,a.NetPrem,b.restartdate,b.reenddate,b.enddate,
           a.RIPrem, a.GEarnedPrem, a.NEarnedPrem,a.GPortfPrem, a.NPortfPrem,a.risum,
           a.insurancetype,a.interestinsured,a.deductibles,a.conditions,a.remarks
     from REINS_REENDR_FAC a,REINS_REENDOR b
    where a.RepolicyNo=b.RepolicyNo
      and a.reendortimes=b.reendortimes
      and a.RecTimes=b.RecTimes
      and b.Status='1'
      and b.RepolicyNo=rec_edr.repolicyno
      and p_Reendortimes<>'000'
      and b.Reendortimes=p_Reendortimes;
   rec_last_re  cur_last_re%rowtype;

cursor cur_fac is
   select * from REINS_REENDR_FAC
   where repolicyno=p_repolicyno
     and RecTimes=p_Rectimes
     and reendortimes = p_reendortimes;
 rec_fac cur_fac%rowType;

cursor cur_plan(v_INSTMARK REINS_REPOLICY.INSTMARK%type,v_chgGrsPrem REINS_REPOLICY.grsprem%type,v_EndorTimes REINS_REPOLICY.Endortimes%type)is
     select 1 PayNo,rec_edr.validdate PlanDate,1 planrate
       from dual
      where v_INSTMARK='N'
    union
     select payno,PlanDate, decode(v_chgGrsPrem,0,1,ChgGRSPLANFEE/v_chgGrsPrem) planrate
       from REINS_ENDOR_UNIT_PLAN
      where v_INSTMARK='Y'
        and PolicyNo=rec_edr.PolicyNo
        and EndorTimes=rec_edr.EndorTimes
        and DangerUnitNo=rec_edr.DangerUnitNo
        and ReRiskCode=rec_edr.ReRiskCode
        and RiskCode=rec_edr.RiskCode
        and v_endortimes<>'000'
    union
     select payno,PlanDate,GRSPLANFEE/v_chgGrsPrem  planrate
       from REINS_POLICY_UNIT_PLAN
      where v_INSTMARK='Y'
        and PolicyNo=rec_edr.PolicyNo
        and DangerUnitNo=rec_edr.DangerUnitNo
        and ReRiskCode=rec_edr.ReRiskCode
        and RiskCode=rec_edr.RiskCode
        and v_endortimes='000';
  rec_plan cur_plan%rowtype;

cursor cur_sumPlan is
    select reinscode,brokercode,confertype,sum(chgplanfee) as chgplanfee
    from REINS_REENDR_FAC_PLAN
    where repolicyno=p_repolicyno
     and rectimes = p_Rectimes
     and reendortimes = p_reendortimes
     group by reinscode,brokercode,confertype;
   rec_sumPlan cur_sumPlan%rowtype;


    v_arr_re_abs arr_re_abs;
    v_termi_rate REINS_REPLY_SHARE.ExchRate%type;--ԭ�������ڱ���
    v_short_rate REINS_REPLY_SHARE.ExchRate%type; --���ڷ��ʣ����Ƿֱ������뱣�����ڲ�һ�����
    v_last_Reendortimes REINS_REPOLICY.reendortimes%type;
    v_char char(1); --�����쳣��ֹ
    v_error_code varchar(2):='00';
    n_seq number(3);
    n_seq1 number(3);
    n_seq2 number(3);
    n_cnt number(3);
    n_newcount number(3);
    FindFlag varchar2(1);
    v_Status REINS_REPOLICY.status%type;
    v_Grslastearned   REINS_REPOLICY.pml%type:=0;
    v_Netlastearned   REINS_REPOLICY.pml%type:=0;
    v_RINetPrem REINS_REPLY_SHARE.Riprem%type;--�ٱ����ֳ�����
    v_INSTMARK REINS_REPOLICY.instmark%type;

    v_endortimes REINS_REENDOR.Endortimes%type;
    v_chgGrsPrem REINS_REPOLICY.Pml%type;
  v_repolicyno reins_repolicy.repolicyno%type;
   v_RiPremLastEarned REINS_REPLY_SHARE.Riprem%type;  -- Mantis 0003500 modify by wuwp 2016-08-25
begin
  open cur_edr;
     fetch cur_edr into rec_edr;
    if cur_edr%notfound then
      v_error_Code:='01';
      g_errcode:='B2201';
      g_errmsg:='�ٷַ����������Ҳ���������';
      close cur_edr;
      select '*' into v_char from dual where 1=2;
    end if;
   close cur_edr;

   open cur_tty(to_number(to_char(rec_edr.restartdate,'yyyy')));
   fetch cur_tty into
      v_arr_re_abs(1).ttyid,v_arr_re_abs(1).StatClass,
      v_arr_re_abs(1).TtyType,v_arr_re_abs(1).ttycode,v_arr_re_abs(1).Uwyear;
   close cur_tty;

  g_errcode:='B2202';
  g_errmsg:='�ٷַ����������ϴηֱ��ٱ����Ĵ���';
--��ʼȡ�ϴηֱ�������õ��ϴηֱ����������ڱ���
  v_last_Reendortimes:=get_last_EndorTimes(rec_edr.repolicyno,rec_edr.reendortimes,rec_edr.restartdate);
  v_Status:=get_last_Status(rec_edr.repolicyno,v_last_Reendortimes);
  if v_Status<>'1' then
      g_errcode:='B2203';
      g_errmsg:='�ٷַ����������ϴηֱ�״̬';
      select '*' into v_char from dual where 1=2;
  end if;
  if v_last_Reendortimes<>'-1' then
    n_cnt:=1;
    for rec_last_re in cur_last_re(v_last_Reendortimes) loop
      v_last_arr_fac_share(n_cnt).ConferType:=rec_last_re.ConferType;
      v_last_arr_fac_share(n_cnt).BrokerCode:=rec_last_re.BrokerCode;
      v_last_arr_fac_share(n_cnt).ReinsCode:=rec_last_re.ReinsCode;
      v_last_arr_fac_share(n_cnt).sharerate:=rec_last_re.sharerate;
      v_last_arr_fac_share(n_cnt).RICurr:=rec_last_re.RICurr;
      v_last_arr_fac_share(n_cnt).ExchRate:=rec_last_re.ExchRate;
      v_last_arr_fac_share(n_cnt).NetInd:=rec_last_re.NetInd;
      v_last_arr_fac_share(n_cnt).GPortfPrem:=rec_last_re.GPortfPrem;
      v_last_arr_fac_share(n_cnt).NPortfPrem:=rec_last_re.NPortfPrem;
      v_termi_rate:=(trunc(rec_edr.restartdate)-trunc(rec_last_re.restartdate))/(trunc(rec_last_re.enddate)+1-trunc(rec_last_re.restartdate));  --modify  by  wuwp 20160721
      --v_termi_rate:=(rec_edr.restartdate-rec_last_re.restartdate)/(rec_last_re.enddate-rec_last_re.restartdate);
      --v_termi_rate := 1;  --modify  by  wuwp 20160721
      v_last_arr_fac_share(n_cnt).GEarnedPrem:=rec_last_re.GEarnedPrem+rec_last_re.GPortfPrem*v_termi_rate;
      v_last_arr_fac_share(n_cnt).NEarnedPrem:=rec_last_re.NEarnedPrem+rec_last_re.NPortfPrem*v_termi_rate;
      v_last_arr_fac_share(n_cnt).NetPrem:=rec_last_re.NetPrem;
      v_last_arr_fac_share(n_cnt).GrsPrem:=rec_last_re.GrsPrem;
      v_last_arr_fac_share(n_cnt).RIPrem:=rec_last_re.RIPrem;
      v_last_arr_fac_share(n_cnt).RISum:=rec_last_re.RISum;
      v_last_arr_fac_share(n_cnt).INSURANCETYPE := rec_last_re.insurancetype;
      v_last_arr_fac_share(n_cnt).deductibles := rec_last_re.deductibles;
      v_last_arr_fac_share(n_cnt).conditions := rec_last_re.conditions;
      v_last_arr_fac_share(n_cnt).remarks := rec_last_re.remarks;
      v_last_arr_fac_share(n_cnt).interestinsured := rec_last_re.interestinsured;
      n_cnt:=n_cnt+1;
    end loop;

--modify  by  wuwp 20160721 begin
    get_last_ply_earned(rec_edr.repolicyno,
                        rec_edr.reendortimes,
                        rec_edr.restartdate,
                        v_Grslastearned,
                        v_NetLastEarned);
    --v_Grslastearned := 0;
    --v_NetLastEarned := 0;
    v_repolicyno:=rec_edr.repolicyno;  -- Mantis 0003500 modify by wuwp 2016-08-25

  else
  --���� add by liupeng 2016-07-27
  --��������ٷֱַ��������߱���δ���ٷ֣������������ٷֳַ������ܻ�ȡ���ڱ��ѵ����
  --�ֱ����Ŵ���Լ�ֱ�����
  select t.repolicyno
    into v_repolicyno
    from reins_repolicy t
   where t.policyno = rec_edr.policyno
     and t.dangerunitno = rec_edr.dangerunitno
     and t.reriskcode = rec_edr.reriskcode
     and t.riskcode = rec_edr.riskcode
     and t.reinstype = '0'
     and rownum=1;
    get_last_ply_earned(v_repolicyno,
                        rec_edr.reendortimes,
                        rec_edr.restartdate,
                        v_Grslastearned,
                        v_NetLastEarned);
  end if;
  v_short_rate := 1;
 if rec_edr.enddate=rec_edr.validdate then
  v_short_rate:=1;
 else
  v_short_rate:=(trunc(rec_edr.enddate)-trunc(rec_edr.restartdate)+1)/(trunc(rec_edr.enddate)-trunc(rec_edr.validdate)+1);
 end if;
 --modify  by  wuwp 20160721 end
  n_cnt:=1;
 for rec_edr_share in cur_edr_share loop
   v_arr_fac_share(n_cnt).ConferType:=rec_edr_share.ConferType;
   v_arr_fac_share(n_cnt).BrokerCode:=rec_edr_share.BrokerCode;
   v_arr_fac_share(n_cnt).ReinsCode:=rec_edr_share.ReinsCode;
   v_arr_fac_share(n_cnt).sharerate:=rec_edr_share.sharerate;
   v_arr_fac_share(n_cnt).RICurr:=rec_edr_share.RICurr;
   v_arr_fac_share(n_cnt).ExchRate:=rec_edr_share.ExchRate;
   v_arr_fac_share(n_cnt).NetInd:=rec_edr_share.NetInd;
   --v_arr_fac_share(n_cnt).ChgRISum:=rec_edr.ChgPml*rec_edr_share.ExchRate*rec_edr_share.sharerate/100;���ǰ��û���ٷֺ��������ٷ�
   v_arr_fac_share(n_cnt).ChgRISum:=rec_edr.PML*v_arr_fac_share(n_cnt).ExchRate*v_arr_fac_share(n_cnt).sharerate/100;
   v_arr_fac_share(n_cnt).rirate:=rec_edr_share.riRate;

   if v_arr_fac_share(n_cnt).riRate!=0 then
      --modify  by  wuwp 20160721
      -- Mantis 0003500 modify by wuwp 2016-08-25 begin
      /*v_arr_fac_share(n_cnt).GPortfPrem:=((rec_edr.pml*v_arr_fac_share(n_cnt).rirate/100-v_Grslastearned-rec_edr.ChgPml*v_arr_fac_share(n_cnt).rirate/100)+rec_edr.ChgPml*v_arr_fac_share(n_cnt).rirate*v_short_rate/100)*v_arr_fac_share(n_cnt).sharerate*v_arr_fac_share(n_cnt).exchrate /100;
      */
       -- ȡ�ϴ��ٷֱַ�δ���ڱ���
     get_last_fac_rirate_earned(v_repolicyno,
                                rec_edr.reendortimes,
                                rec_edr.restartdate,
                                v_arr_fac_share(n_cnt).BrokerCode,
                                v_arr_fac_share(n_cnt).ReinsCode,
                                v_RiPremLastEarned
                       );
      /*v_arr_fac_share(n_cnt).GPortfPrem:=(rec_edr.pml*v_arr_fac_share(n_cnt).rirate/100-v_Grslastearned)*v_arr_fac_share(n_cnt).sharerate*v_arr_fac_share(n_cnt).exchrate /100;
     */
     v_arr_fac_share(n_cnt).GPortfPrem:=rec_edr.PML*v_arr_fac_share(n_cnt).rirate/100*v_arr_fac_share(n_cnt).ExchRate*v_arr_fac_share(n_cnt).sharerate/100*(trunc(rec_edr.enddate)-trunc(rec_edr.validdate)+1)/(trunc(rec_edr.enddate)-trunc(rec_edr.startdate)+1);
      -- Mantis 0003500 modify by wuwp 2016-08-25 end
      /*v_arr_fac_share(n_cnt).NPortfPrem:=((rec_edr.pml*v_arr_fac_share(n_cnt).rirate*rec_edr.netprem/(rec_edr.grsprem*100)-v_Netlastearned-rec_edr.ChgPml*v_arr_fac_share(n_cnt).rirate*rec_edr.netprem/(rec_edr.grsprem*100))+rec_edr.ChgPml*v_arr_fac_share(n_cnt).rirate*v_short_rate*rec_edr.netprem/(rec_edr.grsprem*100)*v_short_rate)*v_arr_fac_share(n_cnt).sharerate*v_arr_fac_share(n_cnt).exchrate /100;
      ----
      v_arr_fac_share(n_cnt).GPortfPrem:=(((rec_edr.pml-rec_edr.ChgPml)*v_arr_fac_share(n_cnt).rirate/100-v_Grslastearned)+rec_edr.ChgPml*v_arr_fac_share(n_cnt).rirate*v_short_rate/100)*v_arr_fac_share(n_cnt).sharerate*v_arr_fac_share(n_cnt).exchrate /100;
      v_arr_fac_share(n_cnt).NPortfPrem:=((rec_edr.pml*v_arr_fac_share(n_cnt).rirate*rec_edr.netprem/(rec_edr.grsprem*100)-v_Netlastearned-rec_edr.ChgPml*v_arr_fac_share(n_cnt).rirate*rec_edr.netprem/(rec_edr.grsprem*100))+rec_edr.ChgPml*v_arr_fac_share(n_cnt).rirate*v_short_rate*rec_edr.netprem/(rec_edr.grsprem*100)*v_short_rate)*v_arr_fac_share(n_cnt).sharerate*v_arr_fac_share(n_cnt).exchrate /100;
      ----
      v_arr_fac_share(n_cnt).GPortfPrem:=((rec_edr.pml*v_arr_fac_share(n_cnt).rirate/100)*v_short_rate\**rec_edr_share.ShortRateMole/rec_edr_share.ShortRatedeNo*\)*v_arr_fac_share(n_cnt).sharerate*v_arr_fac_share(n_cnt).exchrate /100;*/
      --modify  by  wuwp 20160721 end
      if rec_edr.grsprem!=0 then
      v_arr_fac_share(n_cnt).NPortfPrem:=v_arr_fac_share(n_cnt).GPortfPrem * (rec_edr.netprem/rec_edr.grsprem);
      else
        v_arr_fac_share(n_cnt).NPortfPrem:=v_arr_fac_share(n_cnt).GPortfPrem*(rec_edr.chgnetprem/rec_edr.chggrsprem);
      end if;

   else
   --modify  by  wuwp 20160721 begin
      /*v_arr_fac_share(n_cnt).GPortfPrem:=((rec_edr.grsprem-rec_edr.ChgGrsPrem-v_Grslastearned)+rec_edr.ChgGrsPrem*v_short_rate)*v_arr_fac_share(n_cnt).sharerate*v_arr_fac_share(n_cnt).exchrate /100;
      v_arr_fac_share(n_cnt).NPortfPrem:=((rec_edr.NetPrem-rec_edr.ChgNetPrem-v_NetLastEarned)+rec_edr.ChgNetPrem*v_short_rate)*v_arr_fac_share(n_cnt).sharerate*v_arr_fac_share(n_cnt).exchrate /100;*/
      v_arr_fac_share(n_cnt).GPortfPrem:=(rec_edr.grsprem-v_Grslastearned)*v_arr_fac_share(n_cnt).sharerate*v_arr_fac_share(n_cnt).exchrate /100;
      v_arr_fac_share(n_cnt).NPortfPrem:=(rec_edr.NetPrem-v_NetLastEarned)*v_arr_fac_share(n_cnt).sharerate*v_arr_fac_share(n_cnt).exchrate /100;
   --modify  by  wuwp 20160721  end
   end if;

   v_arr_fac_share(n_cnt).GEarnedPrem:=0;
   v_arr_fac_share(n_cnt).NearnedPrem:=0;
   v_arr_fac_share(n_cnt).GrsPrem:=v_arr_fac_share(n_cnt).GEarnedPrem+v_arr_fac_share(n_cnt).GPortfPrem;
   v_arr_fac_share(n_cnt).NetPrem:=v_arr_fac_share(n_cnt).NEarnedPrem+v_arr_fac_share(n_cnt).NPortfPrem;
   v_arr_fac_share(n_cnt).ChgGrsPrem:=v_arr_fac_share(n_cnt).GrsPrem;
   v_arr_fac_share(n_cnt).ChgNetPrem:=v_arr_fac_share(n_cnt).NetPrem;
    if rec_edr_share.netind='0' then
      v_arr_fac_share(n_cnt).RIPrem:=v_arr_fac_share(n_cnt).GrsPrem;
      v_arr_fac_share(n_cnt).ChgRIPrem:=v_arr_fac_share(n_cnt).ChgGrsPrem;
    else
      v_arr_fac_share(n_cnt).RIPrem:=v_arr_fac_share(n_cnt).NetPrem;
      v_arr_fac_share(n_cnt).ChgRIPrem:=v_arr_fac_share(n_cnt).ChgNetPrem;
    end if;
   n_cnt:=n_cnt+1;
  end loop;

   g_errcode:='B2204';
   g_errmsg:='�ٷַ���������ϲ��ϴηֱ��ͱ��ηֱ��������';
--�ϲ��ϴηֱ��ͱ��ηֱ������
     for n_seq2 in 1..v_last_arr_fac_share.count loop
        FindFlag:='N';
        n_newcount:=v_arr_fac_share.count;
      for  n_seq1 in 1..v_arr_fac_share.count loop
        if v_last_arr_fac_share(n_seq2).confertype=v_arr_fac_share(n_seq1).confertype and
          nvl(v_last_arr_fac_share(n_seq2).BrokerCode,'*')=nvl(v_arr_fac_share(n_seq1).BrokerCode,'*') and
          v_last_arr_fac_share(n_seq2).ReinsCode=v_arr_fac_share(n_seq1).ReinsCode then
          --modify  by  wuwp 20160721 begin
    v_arr_fac_share(n_seq1).GEarnedPrem :=v_last_arr_fac_share(n_seq2).GEarnedPrem;
          v_arr_fac_share(n_seq1).NEarnedPrem :=v_last_arr_fac_share(n_seq2).NEarnedPrem;
          /*v_arr_fac_share(n_seq1).GEarnedPrem :=0;
          v_arr_fac_share(n_seq1).NEarnedPrem :=0;*/
    --modify  by  wuwp 20160721 end
          v_arr_fac_share(n_seq1).ChgGrsPrem:=v_arr_fac_share(n_seq1).GEarnedPrem+v_arr_fac_share(n_seq1).GPortfPrem-v_last_arr_fac_share(n_seq2).grsprem;
          v_arr_fac_share(n_seq1).ChgNetPrem:=v_arr_fac_share(n_seq1).NEarnedPrem+v_arr_fac_share(n_seq1).NPortfPrem-v_last_arr_fac_share(n_seq2).netprem;
          v_arr_fac_share(n_seq1).GrsPrem:=v_arr_fac_share(n_seq1).GEarnedPrem+v_arr_fac_share(n_seq1).GPortfPrem;
          v_arr_fac_share(n_seq1).NetPrem:=v_arr_fac_share(n_seq1).NEarnedPrem+v_arr_fac_share(n_seq1).NPortfPrem;
          --���ǰ�����ٷֺ�����ֳ�����仯��
          --modify by liupeng 20160630
          --����仯��=���α���-�ϴα��� �������Ǳ���仯��*�ֳ�����
          --v_arr_fac_share(n_seq1).ChgRISum:=rec_edr.ChgPml*v_arr_fac_share(n_seq1).ExchRate*v_arr_fac_share(n_seq1).sharerate/100;
          v_arr_fac_share(n_seq1).ChgRISum:=rec_edr.Pml*v_arr_fac_share(n_seq1).ExchRate*v_arr_fac_share(n_seq1).sharerate/100-v_last_arr_fac_share(n_seq2).risum;

          if v_arr_fac_share(n_seq1).netInd='0' then --ë����
            v_arr_fac_share(n_seq1).ChgRIPrem:=v_arr_fac_share(n_seq1).ChgGrsPrem;
            v_arr_fac_share(n_seq1).RIPrem:=v_arr_fac_share(n_seq1).GrsPrem;
          elsif v_arr_fac_share(n_seq1).netInd='1' then --������
            v_arr_fac_share(n_seq1).ChgRIPrem:=v_arr_fac_share(n_seq1).ChgNetPrem;
            v_arr_fac_share(n_seq1).RIPrem:=v_arr_fac_share(n_seq1).NetPrem;
          end if;
          v_arr_fac_share(n_seq1).ChgRIPrem:=v_arr_fac_share(n_seq1).RIPrem-v_last_arr_fac_share(n_seq2).RIprem;
          FindFlag:='Y';
        end if;
      end loop;

      /*if findFlag='N' then
        v_arr_fac_share(n_newcount+1).ConferType:=v_last_arr_fac_share(n_seq2).ConferType;
        v_arr_fac_share(n_newcount+1).BrokerCode:=v_last_arr_fac_share(n_seq2).BrokerCode;
        v_arr_fac_share(n_newcount+1).ReinsCode :=v_last_arr_fac_share(n_seq2).ReinsCode;
        v_arr_fac_share(n_newcount+1).sharerate:=0;
        v_arr_fac_share(n_newcount+1).RICurr:=v_last_arr_fac_share(n_seq2).RICurr;
        v_arr_fac_share(n_newcount+1).ExchRate:=v_last_arr_fac_share(n_seq2).ExchRate;
        v_arr_fac_share(n_newcount+1).NetInd:=v_last_arr_fac_share(n_seq2).NetInd;
        v_arr_fac_share(n_newcount+1).GPortfPrem:=0;
        v_arr_fac_share(n_newcount+1).NPortfPrem:=0;
        v_arr_fac_share(n_newcount+1).GEarnedPrem:=v_last_arr_fac_share(n_seq2).GEarnedPrem;
        v_arr_fac_share(n_newcount+1).NEarnedPrem:=v_last_arr_fac_share(n_seq2).NEarnedPrem;
        v_arr_fac_share(n_newcount+1).GrsPrem:=v_last_arr_fac_share(n_seq2).GEarnedPrem;
        v_arr_fac_share(n_newcount+1).NetPrem:=v_last_arr_fac_share(n_seq2).NEarnedPrem;
        if v_arr_fac_share(n_newcount+1).NetInd=0 then
           v_arr_fac_share(n_newcount+1).RIPrem:=v_arr_fac_share(n_newcount+1).GrsPrem;
        else
           v_arr_fac_share(n_newcount+1).RIPrem:=v_arr_fac_share(n_newcount+1).NetPrem;
        end if;
        v_arr_fac_share(n_newcount+1).RISum:=0;
      end if;*/

    end loop;

  n_seq:=1;
  g_errcode:='B2205';
  g_errmsg:='�ٷַ�����������·������ٷֱַ��������';
--  �������������ٷ��ٱ�����Ϣ����
  for n_seq in 1..v_arr_fac_share.count loop
  --�����ֳ�����ļ���
   update REINS_REENDR_FAC set risum=rec_edr.PML*v_arr_fac_share(n_seq).ExchRate*v_arr_fac_share(n_seq).sharerate/100,
                          ChgRISum=v_arr_fac_share(n_seq).ChgRISum,
                          Grsprem=v_arr_fac_share(n_seq).Grsprem,
                          NetPrem=v_arr_fac_share(n_seq).NetPrem,
                          GPortfPrem=v_arr_fac_share(n_seq).GPortfPrem,
                          NPortfPrem=v_arr_fac_share(n_seq).NPortfPrem,
                          GEarnedPrem=v_arr_fac_share(n_seq).GEarnedPrem,
                          NEarnedPrem=v_arr_fac_share(n_seq).NEarnedPrem,
                          RIprem=v_arr_fac_share(n_seq).RIprem,
                          ChgGrsPrem=v_arr_fac_share(n_seq).ChgGrsPrem,
                          ChgNetPrem=v_arr_fac_share(n_seq).ChgNetPrem,
                          INSURANCETYPE = v_arr_fac_share(n_seq).INSURANCETYPE,
                          INTERESTINSURED = v_arr_fac_share(n_seq).INTERESTINSURED,
                          DEDUCTIBLES = v_arr_fac_share(n_seq).DEDUCTIBLES,
                          CONDITIONS = v_arr_fac_share(n_seq).CONDITIONS,
                          remarks = v_arr_fac_share(n_seq).remarks,
                          ChgRIPrem=v_arr_fac_share(n_seq).ChgRIPrem,date_updated = sysdate
     where RePolicyNo=p_RePolicyNo
       and reendortimes=p_reendortimes
       and RecTimes=p_Rectimes
       and ConferType=v_arr_fac_share(n_seq).ConferType
       and ReinsCode=v_arr_fac_share(n_seq).ReinsCode
       and nvl(BrokerCode,'*')=nvl(v_arr_fac_share(n_seq).BrokerCode,'*');
   --modify by liupeng 20170223
   --ԭ���ٱ�������=�ֳ�����*(�����ѱ���+��˰����+�������ñ���)
   --�޸ĺ��ٱ�������=�ֳ�����*(�����ѱ���+�������ñ���)
   --������ֵ˰����ֵ˰�仯���ļ���
   update REINS_REENDR_FAC
      set RICOMM       = RIPrem * (CommRate /*+TaxRate*/
                          +othRate) / 100,
          ChgRICOMM    = ChgRIPrem * (CommRate /*+TaxRate*/
                          +othRate) / 100,
          tax          = RIPrem *
                         /*get_exchrate(ricurr, 'CNY', rec_edr.startdate) **/
                         taxrate / 100,
          changetax    = ChgRIPrem *
                         /*get_exchrate(ricurr, 'CNY', rec_edr.startdate) **/
                         taxrate / 100,
          date_updated = sysdate
    where RePolicyNo = p_RePolicyNo
      and reendortimes = p_reendortimes
      and RecTimes = p_Rectimes
      and ConferType = v_arr_fac_share(n_seq).ConferType
      and ReinsCode = v_arr_fac_share(n_seq).ReinsCode
      and nvl(BrokerCode, '*') =
          nvl(v_arr_fac_share(n_seq).BrokerCode, '*');

   --modify by liupeng 20170223
   --�����������ֵ˰����ֵ˰���Ͳ�ΪӦ˰������ֵ˰Ϊ0
   update REINS_REENDR_FAC t
      set t.tax = 0,
          t.changetax = 0
    where t.repolicyno = p_repolicyno
      and t.reendortimes = p_reendortimes
      and t.RecTimes = p_Rectimes
      and (t.taxind <> '1' or t.payind = 'N');
 end loop;

  g_errcode:='B2206';
  g_errmsg:='��ȡ��������������REINS_REENDR_FAC';
 --����ֱ����share��
  select sum(risum/exchrate),sum(grsprem/exchrate),sum(netprem/exchrate),sum(RIPrem/exchrate),
         sum(Chgrisum/exchrate),sum(Chggrsprem/exchrate),sum(Chgnetprem/exchrate),sum(ChgRIPrem/exchrate),
         sum(GEARNEDPREM/exchrate),sum(GPORTFPREM/exchrate),sum(NEARNEDPREM/exchrate),sum(NPORTFPREM/exchrate),
         sum(ricomm/exchrate),sum(chgricomm/exchrate)
         into
         v_arr_re_abs(1).risum,v_arr_re_abs(1).grsprem,v_arr_re_abs(1).netprem,v_arr_re_abs(1).riprem,
         v_arr_re_abs(1).Chgrisum,v_arr_re_abs(1).Chggrsprem,v_arr_re_abs(1).Chgnetprem,v_arr_re_abs(1).Chgriprem,
         v_arr_re_abs(1).GEARNEDPREM,v_arr_re_abs(1).GPORTFPREM,v_arr_re_abs(1).NEARNEDPREM,v_arr_re_abs(1).NPORTFPREM,
         v_arr_re_abs(1).ricomm,v_arr_re_abs(1).chgricomm
     from REINS_REENDR_FAC
     where repolicyno=p_repolicyno
       and reendortimes=p_reendortimes
       and RecTimes=p_RecTimes;

  v_arr_re_abs(1).SectNo    :='0';
  v_arr_re_abs(1).ShareRate :=rec_edr.sharerate;
  v_arr_re_abs(1).RICurr    :=rec_edr.currency;
  v_arr_re_abs(1).ExchRate  :=1;
  select pcind,NetInd into v_arr_re_abs(1).PCind,v_arr_re_abs(1).NetInd
    from REINS_REENDR_FAC
   where repolicyno=p_repolicyno
   and reendortimes=p_reendortimes
   and RecTimes=p_RecTimes
   and rownum=1;

  g_errcode:='B2207';
  g_errmsg:='�ٷַ�����������������ֳ����REINS_REENDR_SHARE����';
  crt_edr_abs(p_repolicyno,p_reendortimes,p_RecTimes,v_Arr_Re_Abs);

  if rec_edr.ChgGrsPrem=0 then
   v_INSTMARK:='N';
  else
   v_INSTMARK:=rec_edr.INSTMARK;
  end if;

  g_errmsg:='ȡΣ�յ�λë����';
  v_endortimes := lpad(nvl(rec_edr.endortimes,'000'),3,'0');
  /*select lpad(nvl(endortimes,'000'),3,'0') into v_endortimes
        from REINS_REENDOR where repolicyno=p_repolicyno and rectimes=p_rectimes and reendortimes=p_reendortimes;*/
  if v_endortimes<>'000' then
    select nvl(Chggrsprem,0) into v_chgGrsPrem
     from REINS_ENDOR_UNIT
    where PolicyNo=rec_edr.PolicyNo
      and EndorTimes=rec_edr.EndorTimes
      and DangerUnitNo=rec_edr.DangerUnitNo
      and ReRiskCode=rec_edr.ReRiskCode
      and RiskCode=rec_edr.RiskCode;
   else      --ȡΣ�յ�λë����
    select GrsPrem into v_chgGrsPrem
      from REINS_POLICY_UNIT
     where PolicyNo=rec_edr.PolicyNo
       and DangerUnitNo=rec_edr.DangerUnitNo
       and ReRiskCode=rec_edr.ReRiskCode
       and RiskCode=rec_edr.RiskCode;
   end if;

   open cur_plan(v_INSTMARK,v_chgGrsPrem,v_endortimes);
   fetch cur_plan into rec_plan;
      if cur_plan%notfound then
         g_errmsg:='����û�з�����Ϣ';
         close cur_plan;
         select '*' into v_char from dual where 1=2;
      end if;
   close cur_plan;

   g_errcode:='B2208';
   g_errmsg:='�ٷַ�������������ٷַ����ɷѼƻ�REINS_REENDR_FAC_PLAN����';
   for rec_plan in cur_plan(v_INSTMARK,v_chgGrsPrem,v_endortimes) loop
   for rec_fac in cur_fac loop
    insert into REINS_REENDR_FAC_PLAN
      (REPOLICYNO,
       REENDORTIMES,
       PAYNO,
       RECTIMES,
       CONFERTYPE,
       BROKERCODE,
       REINSCODE,
       PAYDATE,
       CURRENCY,
       CHGPLANFEE,
       REMARKS)
      select rec_fac.REPOLICYNO,
             rec_fac.reendortimes,
             rec_plan.PAYNO,
             rec_fac.RECTIMES,
             rec_fac.ConferType,
             rec_fac.BROKERCODE,
             rec_fac.ReinsCode,
             rec_plan.plandate,
             rec_fac.Ricurr,
             --modiby by liupeng �ٱ�Ӫ���� �ٱ��ֱ��������㷨����
             --���Ϊ�Ǵ��۴��� ������=�ֳ�����+��ֵ˰ - ������
             --���Ϊ���۴���  ������=�ֳ����� - ������ - ����˰ ������˰=��ֵ˰*12%��
             (rec_fac.ChgRIPrem - rec_fac.ChgRIComm +
             decode(rec_fac.collectind, 'Y', 0, nvl(rec_fac.ChangeTax, 0)) -
             decode(rec_fac.collectind,
                     'Y',
                     nvl(rec_fac.ChangeTax, 0) * 0.12,
                     0)) * rec_plan.planrate,
             ''
        from dual;
   end loop;
  end loop;
  --���ڸ������
  for rec_sumPlan in cur_sumPlan Loop
      select chgriprem - chgricomm +
             decode(collectind, 'Y', 0, nvl(ChangeTax, 0)) -
             decode(collectind,
                    'Y',
                    nvl(ChangeTax, 0) * 0.12,
                    0)
        into v_RINetPrem
        from REINS_REENDR_FAC
       where repolicyno = p_repolicyno
         and rectimes = p_rectimes
         and reendortimes = p_reendortimes
         and reinscode = rec_sumPlan.reinscode
         and confertype = rec_sumPlan.confertype
         and nvl(brokercode, '*') = nvl(rec_sumPlan.brokerCode, '*');

    update REINS_REENDR_FAC_PLAN
    set chgplanfee = chgplanfee+v_riNetPrem - rec_sumPlan.chgplanfee
    where repolicyno=p_repolicyno
        and rectimes = p_rectimes
        and reendortimes=p_reendortimes
        and reinscode = rec_sumPlan.reinscode
        and nvl(brokercode,'*')=nvl(rec_sumPlan.brokerCode,'*')
        and confertype = rec_sumPlan.confertype
        and rownum=1;
  end loop;

  p_message_code :='0';
  p_message_desc:='����ɹ�';
  update REINS_REENDOR set status='1',caldate=sysdate,date_updated=sysdate
   where repolicyno=p_repolicyno
     and reendortimes=p_reendortimes
     and rectimes=p_rectimes;

  update reins_reendr_fac t
     set t.insurancetype  =
         (select a.insurancetype
            from reins_reply_fac a
           where a.repolicyno = t.repolicyno
             and nvl(a.brokercode, '*') = nvl(t.brokercode, '*')
             and a.reinscode = t.reinscode
             and a.rectimes = t.rectimes),
         t.interestinsured =
         (select a.interestinsured
            from reins_reply_fac a
           where a.repolicyno = t.repolicyno
             and nvl(a.brokercode, '*') = nvl(t.brokercode, '*')
             and a.reinscode = t.reinscode
             and a.rectimes = t.rectimes),
         t.conditions     =
         (select a.conditions
            from reins_reply_fac a
           where a.repolicyno = t.repolicyno
             and nvl(a.brokercode, '*') = nvl(t.brokercode, '*')
             and a.reinscode = t.reinscode
             and a.rectimes = t.rectimes),
         t.remarks        =
         (select a.remarks
            from reins_reply_fac a
           where a.repolicyno = t.repolicyno
             and nvl(a.brokercode, '*') = nvl(t.brokercode, '*')
             and a.reinscode = t.reinscode
             and a.rectimes = t.rectimes),
         t.deductibles    =
         (select a.deductibles
            from reins_reply_fac a
           where a.repolicyno = t.repolicyno
             and nvl(a.brokercode, '*') = nvl(t.brokercode, '*')
             and a.reinscode = t.reinscode
             and a.rectimes = t.rectimes)
   where t.repolicyno = p_repolicyno
     and t.reendortimes = p_reendortimes
     and t.rectimes = p_rectimes;
 commit;

 exception when others then
   if v_error_Code='01' then
      p_message_code:='0';
      p_message_desc:='�˷������Ѽ���������';
   else
      p_message_code:='1';
      p_message_desc:='����ʧ��';
   end if;
   --���������־
   g_errmsg:=g_errmsg||'-'||substr(sqlerrm,1,100);
   rollback;
   if v_error_Code<>'01' then
     update REINS_REENDOR set status = '2',date_updated=sysdate
      where repolicyno = p_repolicyno
        and reendortimes = p_reendortimes
        and rectimes = p_rectimes;
     insert into REINS_ENDOR_ERR_LOG(errtype,repolicyno,reendortimes,rectimes,errmsg)
                        values('B2',p_repolicyno,p_reendortimes,p_rectimes,g_errmsg);

     commit;
   end if;
end fac_edr_cal;

/* �ٷַ��ⰸ���� */
procedure fac_clm_cal(p_reclaimno in REINS_RECLAIM.reclaimno%type,
                      p_RecTimes  in REINS_RECLAIM.RecTimes%type,
                      p_message_code out varchar2,
                      p_message_desc out varchar2 ) is
cursor cur_tty(p_ply_year number) is
  select a.ttyid,b.StatClass,b.TtyType,a.ttycode,a.Uwyear
    from REINS_TREATY a,REINS_TTY_TABLE b
   where a.Uwyear=p_ply_year
     and a.ttycode=b.ttycode and b.ttycode = 'FAC01'
     and b.ttytype='91'
     ORDER BY a.ttyid;  --�ٷֺ�ԼID
rec_tty cur_tty%rowtype;

cursor cur_clm is
  select * from REINS_RECLAIM
   where reclaimno=p_reclaimno
     and RecTimes=p_Rectimes
     and Status in ('0','2') for update;
rec_clm cur_clm%rowtype;

cursor cur_clmfac is
  select pcind,sum(paidsum) paidsum from REINS_RECLAIM_FAC
   where reclaimno=p_reclaimno
     and RecTimes=p_Rectimes
    group by pcind;
rec_clmfac cur_clmfac%rowtype;
v_reclm_para reclm_para;
v_fac_share  Arr_danger_fac_share;
v_share  Arr_danger_share;
v_char char(1); --�����쳣��ֹ
v_error_code varchar(2):='00';
n_seq number(3);
begin
   open cur_clm;
     fetch cur_clm into rec_clm;
     if cur_clm%notfound then
      v_error_Code:='01';
      g_errcode:='B3201';
      g_errmsg:='�Ѿ��ٷַ��ⰸ�����Ҳ������ⰸ��Ϣ';
      close cur_clm;
      select '*' into v_char from dual where 1=2;
     end if;
   close cur_clm;

  v_reclm_para.RepolicyNo:=rec_clm.repolicyno;
  v_reclm_para.policyNo:=rec_clm.policyno;
  v_reclm_para.riskcode:=rec_clm.riskcode;
  v_reclm_para.reriskcode:=rec_clm.reriskcode;
  v_reclm_para.DANGERUNITNO:=rec_clm.DANGERUNITNO;
  v_reclm_para.Uwyear:=to_number(to_char(rec_clm.startdate,'yyyy'));
  v_reclm_para.Dangertype:=rec_clm.dangertype;
  v_reclm_para.DangerCode:=rec_clm.dangerCode;
  v_reclm_para.DamageDATE:=trunc(rec_clm.damagedate);
  v_reclm_para.currency:=rec_clm.currency;
  v_reclm_para.paidsum:=rec_clm.paidsum;
  --����������ڴ��ڱ���ֹ�ڣ�����ڱ���ֹ��
  if rec_clm.DamageDATE>rec_clm.enddate then
     v_reclm_para.DamageDATE:=rec_clm.enddate;
  end if;
  --�����������С�ڱ������ڣ�����ڱ�������
  if rec_clm.DamageDATE<rec_clm.startDate then
     v_reclm_para.DamageDATE:=rec_clm.startDate;
  end if;

  g_errcode:='B3202';
  g_errmsg:='�Ѿ��ٷַ��ⰸ�������̯�ر�������';
  get_fac_ply_share(v_reclm_para,v_fac_share);
  if v_fac_share.count<=0 then
    g_errmsg :='�Ѿ��ٷַ��ⰸ�Ҳ����ֱ�����';
    select '*' into v_char from dual where 1=2;
  end if;

  g_errcode:='B3203';
  g_errmsg:='�Ѿ��ٷַ��ⰸ��������ٷ�̯�ؽ��REINS_RECLAIM_FAC����';
   for n_seq in 1..v_fac_share.count loop
    insert into REINS_RECLAIM_FAC
      (RECLAIMNO,
       RECTIMES,
       CONFERTYPE,
       BROKERCODE,
       REINSCODE,
       PAYCODE,
       AGENTCODE,
       CONFERNO,
       RIREFNO,
       SHARERATE,
       RICURR,
       EXCHRATE,
       PAIDSUM,
       PCIND,
       date_created,
       date_updated,
       Insurancetype,
       Interestinsured,
       Remarks,
       Conditions,
       Deductibles)
      select rec_clm.RECLAIMNO,
             rec_clm.RECTIMES,
             v_fac_share      (n_seq).CONFERTYPE,
             v_fac_share      (n_seq).BROKERCODE,
             v_fac_share      (n_seq).REINSCODE,
             v_fac_share      (n_seq).PAYCODE,
             v_fac_share      (n_seq).AGENTCODE,
             v_fac_share      (n_seq).CONFERNO,
             v_fac_share      (n_seq).RIREFNO,
             v_fac_share      (n_seq).SHARERATE,
             rec_clm.currency,
             1, --�һ���
             v_fac_share      (n_seq).sharerate * rec_clm.PAIDSUM / 100,
             v_fac_share      (n_seq).PCIND,
             sysdate,
             sysdate,
             v_fac_share      (n_seq).Insurancetype,
             v_fac_share      (n_seq).Interestinsured,
             v_fac_share      (n_seq).Remarks,
             v_fac_share      (n_seq).Conditions,
             v_fac_share      (n_seq).Deductibles
        from dual;
   end loop;

  open cur_tty(to_number(to_char(rec_clm.startdate,'yyyy')));
   fetch cur_tty into  rec_tty;
    n_seq:=1;
     for rec_clmfac in cur_clmfac loop
      v_share(n_Seq).ttyid:=rec_tty.ttyid;
      v_share(n_Seq).sectno:='0';
      v_share(n_seq).PCInd:=rec_clmfac.PCInd;
      v_share(n_seq).SHARERATE:=rec_clmfac.PaidSum*100/rec_clm.PaidSum ;
      v_share(n_seq).RICURR :=rec_clm.currency;
      v_share(n_seq).EXCHRATE :=1;
      v_share(n_seq).PAIDSUM :=rec_clmfac.PaidSum;
      n_seq:=n_seq+1;
    end loop;
   close cur_tty;

  g_errcode:='B3204';
  g_errmsg:='�Ѿ��ٷַ��ⰸ�����γɽ����Ϣ';
  crt_clm_abs(p_reclaimno,p_RecTimes,v_share);

  p_message_code :='0';
  p_message_desc:='����ɹ�';
  update REINS_RECLAIM set status='1',caldate=sysdate,date_updated=sysdate
   where reclaimno=p_reclaimno and rectimes=p_rectimes;
  commit;

  exception when others then
   if v_error_Code='01' then
      p_message_code:='0';
      p_message_desc:='�˷��ⰸ�ѽ��м���';
   else
      p_message_code:='1';
      p_message_desc:='����ʧ��';
   end if;
  --���������־
   g_errmsg:=g_errmsg||'-'|| substr(sqlerrm,1,100);
   rollback;
   if v_error_Code<>'01' then
     update REINS_RECLAIM set status='2',date_updated=sysdate
      where reclaimno=p_reclaimno and rectimes=p_rectimes;
     insert into REINS_CLAIM_ERR_LOG(errtype,reclaimno,rectimes,errmsg)
                        values('B3',p_reclaimno,p_rectimes,g_errmsg);

     commit;
   end if;

end fac_clm_cal;

/* �ٷַ�δ���ⰸ���� */
procedure fac_OSclm_cal(p_osreclaimno in REINS_OS_RECLAIM.osreclaimno%type,
                        p_message_code out varchar2,
                        p_message_desc out varchar2 ) is
cursor cur_tty(p_ply_year number) is
  select a.ttyid,b.StatClass,b.TtyType,a.ttycode,a.Uwyear
    from REINS_TREATY a,REINS_TTY_TABLE b
   where a.Uwyear=p_ply_year
     and a.ttycode=b.ttycode and b.ttycode = 'FAC01'
     and b.ttytype='91'
     ORDER BY a.ttyid;  --�ٷֺ�ԼID
rec_tty cur_tty%rowtype;

cursor cur_clm is
  select * from REINS_OS_RECLAIM
   where OSreclaimno=p_osreclaimno
     and Status in ('0','2') for update;
rec_clm cur_clm%rowtype;

cursor cur_clmfac is
  select pcind,sum(OSsum) paidsum from REINS_OS_RECLAIM_FAC
   where OSreclaimno=p_osreclaimno
    group by pcind;
rec_clmfac cur_clmfac%rowtype;
v_reclm_para reclm_para;
v_fac_share  Arr_danger_fac_share;
v_share  Arr_danger_share;
v_char char(1); --�����쳣��ֹ
v_error_code varchar(2):='00';
n_seq number(3);
begin
   open cur_clm;
     fetch cur_clm into rec_clm;
     if cur_clm%notfound then
      v_error_Code:='01';
      g_errcode:='B4201';
      g_errmsg:='�ٷ�δ�������Ҳ����ֵ���Ϣ';
      select '*' into v_char from dual where 1=2;
     end if;
   close cur_clm;

  v_reclm_para.RepolicyNo:=rec_clm.repolicyno;
  v_reclm_para.policyNo:=rec_clm.policyno;
  v_reclm_para.riskcode:=rec_clm.riskcode;
  v_reclm_para.reriskcode:=rec_clm.reriskcode;
  v_reclm_para.DANGERUNITNO:=rec_clm.DANGERUNITNO;
  v_reclm_para.Uwyear:=to_number(to_char(rec_clm.startdate,'yyyy'));
  v_reclm_para.Dangertype:=rec_clm.dangertype;
  v_reclm_para.DangerCode:=rec_clm.dangerCode;
  v_reclm_para.DamageDATE:=trunc(rec_clm.damagedate);
  v_reclm_para.currency:=rec_clm.currency;
  v_reclm_para.paidsum:=rec_clm.OSsum;
  --����������ڴ��ڱ���ֹ�ڣ�����ڱ���ֹ��
  if rec_clm.DamageDATE>rec_clm.enddate then
     v_reclm_para.DamageDATE:=rec_clm.enddate;
  end if;
  --�����������С�ڱ������ڣ�����ڱ�������
  if rec_clm.DamageDATE<rec_clm.startDate then
     v_reclm_para.DamageDATE:=rec_clm.startDate;
  end if;

  g_errcode:='B4202';
  g_errmsg:='�ٷ�δ��������̯��ռ��';
  get_fac_ply_share(v_reclm_para,v_fac_share);
  if v_fac_share.count<=0 then
    g_errmsg :='δ���ٷַ��ⰸ�Ҳ����ֱ�����';
    select '*' into v_char from dual where 1=2;
  end if;

  g_errcode:='B4203';
  g_errmsg:='�ٷ�δ�������γ�REINS_OS_RECLAIM_FAC�ṹ��Ϣ';
   for n_seq in 1..v_fac_share.count loop
     insert into REINS_OS_RECLAIM_FAC(
         OSRECLAIMNO   ,
         CONFERTYPE  ,
         BROKERCODE  ,
         REINSCODE   ,
         PAYCODE     ,
         AGENTCODE   ,
         CONFERNO    ,
         RIREFNO,
         SHARERATE   ,
         RICURR      ,
         EXCHRATE    ,
         OSSUM     ,
         PCIND       ,
         date_created,
         date_updated)
      select
         rec_clm.OSRECLAIMNO  ,
         v_fac_share(n_seq).CONFERTYPE,
         v_fac_share(n_seq).BROKERCODE,
         v_fac_share(n_seq).REINSCODE ,
         v_fac_share(n_seq).PAYCODE   ,
         v_fac_share(n_seq).AGENTCODE ,
         v_fac_share(n_seq).CONFERNO  ,
         v_fac_share(n_seq).RIREFNO   ,
         v_fac_share(n_seq).SHARERATE ,
         rec_clm.currency ,
         1 ,     --�һ���
         v_fac_share(n_seq).sharerate*rec_clm.OSSUM/100 ,
         v_fac_share(n_seq).PCIND,
         sysdate,
         sysdate
       from dual;
   end loop;

  open cur_tty(to_number(to_char(rec_clm.startdate,'yyyy')));
   fetch cur_tty into  rec_tty;
    n_seq:=1;
     for rec_clmfac in cur_clmfac loop
      v_share(n_Seq).ttyid:=rec_tty.ttyid;
      v_share(n_Seq).sectno:='0';
      v_share(n_seq).PCInd:=rec_clmfac.PCInd;
      v_share(n_seq).SHARERATE:=rec_clmfac.PaidSum*100/rec_clm.OSSum ;
      v_share(n_seq).RICURR :=rec_clm.currency;
      v_share(n_seq).EXCHRATE :=1;
      v_share(n_seq).PAIDSUM :=rec_clmfac.PaidSum;
      n_seq:=n_seq+1;
    end loop;
   close cur_tty;

  g_errcode:='B4204';
  g_errmsg:='�ٷ�δ����������REINS_OS_RECLAIM_SHARE�����ݳ���';
  crt_OSclm_abs(p_osreclaimno,v_share);

  p_message_code :='0';
  p_message_desc:='����ɹ�';
  update REINS_OS_RECLAIM set status='1',caldate=sysdate,date_updated=sysdate
   where OSreclaimno=p_osreclaimno;
  commit;

  exception when others then
   if v_error_Code='01' then
      p_message_code:='0';
      p_message_desc:='��δ���ѽ��м���';
   else
      p_message_code:='1';
        p_message_desc:='����ʧ��';
   end if;
   --������־��¼
   g_errmsg:=g_errmsg||'-'||substr(sqlerrm,1,100);
   rollback;
   if v_error_Code<>'01' then
     update REINS_OS_RECLAIM set status='2',date_updated=sysdate
      where OSreclaimno=p_osreclaimno ;
     insert into REINS_OS_CLAIM_ERR_LOG
       (errtype, osreclaimno,errmsg)
     values
       ('B4', p_osreclaimno,g_errmsg);
     commit;
   end if;
end fac_OSclm_cal;

/* �Ǳ����ٷֱַ������� */
procedure nfac_ply_cal(p_repolicyno   in REINS_REPOLICY.repolicyno%type,
                       p_RecTimes     in REINS_REPOLICY.RecTimes%type,
                       p_message_code out varchar2,
                       p_message_desc out varchar2) is

  cursor cur_tty(p_ply_year number, p_ttytype REINS_TTY_TABLE.ttytype%type) is
    select a.ttyid, b.StatClass, b.TtyType, a.ttycode, a.Uwyear
      from REINS_TREATY a, REINS_TTY_TABLE b
     where a.Uwyear = p_ply_year
       and a.ttycode = b.ttycode
       and b.ttytype = p_ttytype
       ORDER BY a.ttyid;

  cursor cur_ply is
    select *
      from REINS_REPOLICY
     where repolicyno = p_repolicyno
       and RecTimes = p_Rectimes
       and Status in ('0', '2')
       for update;
  rec_ply cur_ply%rowtype;

  cursor cur_nfac is
    select a.LAYERNO, b.BROKERCODE, b.ReinsCode, a.Ricurr, b.RIPREM, b.reinscommamount,b.netprem
      from REINS_REPLY_N_FAC a, REINS_REPLY_N_FAC_REINS b
     where a.repolicyno = p_repolicyno
       and a.RecTimes = p_Rectimes
       and a.repolicyno = b.repolicyno
       and a.RecTimes = b.RecTimes
       and a.Layerno = b.Layerno;
  rec_nfac cur_nfac%rowType;

  cursor cur_plan(v_INSTMARK REINS_REPOLICY.INSTMARK%type, v_oriGrsprem REINS_REPOLICY.Pml%type) is
    select 1 PayNo, rec_ply.startdate PlanDate, 1 planrate
      from dual
     where v_INSTMARK = 'N'
    union
    select payno, PlanDate, GRSPLANFEE / v_oriGrsprem planrate
      from REINS_POLICY_UNIT_PLAN
     where v_INSTMARK = 'Y'
       and PolicyNo = rec_ply.PolicyNo
       and DangerUnitNo = rec_ply.DangerUnitNo
       and ReRiskCode = rec_ply.ReRiskCode
       and RiskCode = rec_ply.RiskCode;
  rec_plan cur_plan%rowtype;

  cursor cur_sumPlan is
    select layerno, reinscode, brokercode, sum(planfee) as planfee
      from REINS_REPLY_N_FAC_PLAN
     where repolicyno = p_repolicyno
       and rectimes = p_Rectimes
     group by layerno, reinscode, brokercode;
  rec_sumPlan cur_sumPlan%rowtype;

  v_sharerate   number;
  v_xlsharerate number;
  v_LeftPrem    REINS_REPOLICY.GRSPREM%type;
  v_oriGrsprem  REINS_REPOLICY.GRSPREM%type;
  v_arr_re_abs  arr_re_abs; --v_arr_re_abs���������Ϊ�˹��ò���ֱ������ķ���
  --v_short_rate REINS_REPLY_SHARE.ExchRate%type:=1; --���ڷ��ʣ����Ƿֱ������뱣�����ڲ�һ�����
  v_char        char(1); --�����쳣��ֹ
  v_error_code  varchar(2) := '00';
  v_SumPrem     REINS_REPLY_SHARE.Riprem%type; --�ٱ��˲�ֳ�����
  --  Mantis 0001087 wuwp 2016-06-20 begin
  v_sum_loss number(16,2):=0; --�ܲ��޶�
  v_sum_RIPrem REINS_REPLY_N_FAC_REINS.Riprem%type;
  v_sum_RiComm REINS_REPLY_N_FAC_REINS.Reinscommamount%type;
  --  Mantis 0001087 wuwp 2016-06-20 end
begin
  open cur_ply;
  fetch cur_ply into rec_ply;
  if cur_ply%notfound then
    v_error_Code := '01';
    close cur_ply;
    g_errcode:='B1301';
    g_errmsg:='�ٷֳ���ֱ��������Ҳ����ֱ���';
    select '*' into v_char from dual where 1 = 2;
  end if;
  close cur_ply;

  v_xlsharerate := rec_ply.sharerate;

  -----------�����ٷּ���----------------��
  v_arr_re_abs(1).PCind := 'P';
  v_arr_re_abs(1).NetInd := '0';
  --���ڷ��ʣ����Ƿ�
  --v_short_rate := (trunc(rec_ply.enddate) - trunc(rec_ply.restartdate) + 1) / (trunc(rec_ply.enddate) + 1 - trunc(rec_ply.startdate));
  --���ֲ��ֳܷ����ѡ�
  g_errmsg:='���ֲ��ֳܷ�����';
  update REINS_REPLY_N_FAC_REINS a
     set a.riprem =
         (select b.premium * a.sharerate/100
            from REINS_REPLY_N_FAC b
           where a.repolicyno = b.repolicyno
             and a.rectimes = b.rectimes
             and a.layerno = b.layerno)
   where a.repolicyno = p_repolicyno
     and a.rectimes = p_RecTimes;
   --modify by wangmx 20160806 begin
   update REINS_REPLY_N_FAC a
     set a.ricomm =
         (select SUM(b.reinscommamount/ a.exchrate)
            from reins_reply_n_fac_reins b
          where a.repolicyno = b.repolicyno
            and a.layerno = b.layerno
            and a.rectimes = b.rectimes)
   where a.repolicyno = p_repolicyno
      and a.rectimes = p_RecTimes;
   --modify by wangmx 20160806 end

--  Mantis 0001087 wuwp 2016-06-20 begin
  select sum(t.riprem / b.exchrate)
    into v_sum_RIPrem
    from REINS_REPLY_N_FAC_REINS t,REINS_REPLY_N_FAC b
   where t.repolicyno = p_repolicyno
     and t.rectimes = p_rectimes
     and t.repolicyno=b.repolicyno
     and t.rectimes=b.rectimes
     and t.layerno=b.layerno;

  select sum(t.reinscommamount / b.exchrate)
    into v_sum_RiComm
    from REINS_REPLY_N_FAC_REINS t,REINS_REPLY_N_FAC b
   where t.repolicyno = p_repolicyno
     and t.rectimes = p_rectimes
     and t.repolicyno=b.repolicyno
     and t.rectimes=b.rectimes
     and t.layerno=b.layerno;

  --�Ǳ����ٷֳ����������ԭ��������ռ�ȼ��㣬��Ϊ���ܲ��޶�ռ�ȼ���
  select sum((t.endloss - t.excessloss) / t.exchrate)
    into v_sum_loss
    from REINS_REPLY_N_FAC t
   where t.repolicyno = p_repolicyno
     and t.rectimes = p_rectimes;
  if v_sum_loss=0 or v_sum_loss is null then
      v_sharerate := 0;
    else
      v_sharerate:=v_sum_loss/rec_ply.pml;
    end if;
  /*select sum(RIPrem / exchrate)
    into v_arr_re_abs(1).RIPrem
    from REINS_REPLY_N_FAC_REINS a,REINS_REPLY_N_FAC b
   where a.repolicyno = p_repolicyno
     and a.RecTimes = p_RecTimes
     and a.layerno=b.layerno
     and a.repolicyno = b.repolicyno
     and a.RecTimes = b.RecTimes;
  if v_arr_re_abs(1).NetInd = '1' then
    v_sharerate := v_arr_re_abs(1).RIPrem / rec_ply.NetPrem;
    v_arr_re_abs(1).GrsPrem := v_arr_re_abs(1).RIPrem * rec_ply.GrsPrem / rec_ply.NetPrem;
    v_arr_re_abs(1).NetPrem := v_arr_re_abs(1).RIPrem;
  else
    v_sharerate := v_arr_re_abs(1).RIPrem / rec_ply.GrsPrem;
    v_arr_re_abs(1).NetPrem := v_arr_re_abs(1).RIPrem * rec_ply.NetPrem / rec_ply.GrsPrem;
    v_arr_re_abs(1).GrsPrem := v_arr_re_abs(1).RIPrem;
  end if;*/
  v_arr_re_abs(1).ricomm:=0;
  v_arr_re_abs(1).RISum := rec_ply.Pml * v_sharerate;
  v_arr_re_abs(1).SectNo := '0';
  v_arr_re_abs(1).ShareRate := v_sharerate * 100;
  v_arr_re_abs(1).RICurr := rec_ply.currency;
  v_arr_re_abs(1).ExchRate := 1;
  v_arr_re_abs(1).GrsPrem := v_sum_RIPrem;
  v_arr_re_abs(1).NetPrem := v_sum_RIPrem;
  v_arr_re_abs(1).RIPrem := v_sum_RIPrem;
  v_arr_re_abs(1).Ricomm := v_sum_RiComm;

--  Mantis 0001087 wuwp 2016-06-20 end

  open cur_tty(to_number(to_char(rec_ply.startdate, 'yyyy')), '92'); --�����ٷֺ�Լ��Ҫ�ġ�
  fetch cur_tty
    into v_arr_re_abs(1).ttyid, v_arr_re_abs(1).StatClass, v_arr_re_abs(1).TtyType, v_arr_re_abs(1).ttycode, v_arr_re_abs(1).Uwyear;
  close cur_tty;

  --modify by liupeng 20160518
  --������������ܲ��޶��������������
  -----------�����ٷֵĲ��ַŵ�����������Լ------------
  /*if v_arr_re_abs(1).NetInd = '1' then
    v_LeftPrem := rec_ply.NetPrem * v_xlsharerate / 100-v_arr_re_abs(1).RIPrem ;
  else
    v_LeftPrem := rec_ply.GrsPrem * v_xlsharerate / 100-v_arr_re_abs(1).RIPrem ;
  end if;
  if abs(v_LeftPrem) >= 0.01 then
    v_arr_re_abs(2).PCind := v_arr_re_abs(1).PCind;
    v_arr_re_abs(2).NetInd := v_arr_re_abs(1).NetInd;
    v_arr_re_abs(2).RISum := rec_ply.Pml * v_xlsharerate / 100-v_arr_re_abs(1).RISum ;
    v_arr_re_abs(2).GrsPrem :=  rec_ply.GrsPrem * v_xlsharerate / 100-v_arr_re_abs(1).GrsPrem ;
    v_arr_re_abs(2).NetPrem := rec_ply.NetPrem * v_xlsharerate / 100-v_arr_re_abs(1).NetPrem  ;
    v_arr_re_abs(2).RIPrem := v_LeftPrem;

    v_arr_re_abs(2).ricomm:=0;
    v_arr_re_abs(2).SectNo := '0';
    v_arr_re_abs(2).ShareRate := v_xlsharerate-v_arr_re_abs(1).ShareRate  ;
    v_arr_re_abs(2).RICurr := v_arr_re_abs(1).RICurr;
    v_arr_re_abs(2).ExchRate := 1;

    open cur_tty(to_number(to_char(rec_ply.startdate, 'yyyy')), '82'); --����������Լ��
    fetch cur_tty
      into v_arr_re_abs(2).ttyid, v_arr_re_abs(2).StatClass, v_arr_re_abs(2).TtyType, v_arr_re_abs(2).ttycode, v_arr_re_abs(2).Uwyear;
    close cur_tty;
  end if;*/

 g_errcode:='B1302';
 g_errmsg:='�ٷֳ���ֱ����������ɷֱ����ֱ����';
  --���ɷֱ����ֱ����
  crt_ply_abs(p_repolicyno, p_RecTimes, v_Arr_Re_Abs);


  -----------�����ٷַ��ڼƻ���------------
 g_errcode:='B1303';
 g_errmsg:='��ȡΣ�յ�λë����';
  select GrsPrem
    into v_oriGrsprem
    from REINS_POLICY_UNIT
   where PolicyNo = rec_ply.PolicyNo
     and DangerUnitNo = rec_ply.DangerUnitNo
     and ReRiskCode = rec_ply.ReRiskCode
     and RiskCode = rec_ply.RiskCode;

   open cur_plan(rec_ply.INSTMARK, v_oriGrsprem);
   fetch cur_plan into rec_plan;
     if cur_plan%notfound then
       g_errmsg:='����û�з�����Ϣ';
       close cur_plan;
       select '*' into v_char from dual where 1=2;
     end if;
   close cur_plan;

  for rec_plan in cur_plan(rec_ply.INSTMARK, v_oriGrsprem) loop
    for rec_nfac in cur_nfac loop
      insert into REINS_REPLY_N_FAC_PLAN
        (REPOLICYNO,
         RECTIMES,
         LAYERNO,
         PAYNO,
         BROKERCODE,
         REINSCODE,
         PAYDATE,
         CURRENCY,
         PLANFEE,
         REMARKS)
        select p_repolicyno,
               p_rectimes,
               rec_nfac.LAYERNO,
               rec_plan.PAYNO,
               rec_nfac.BROKERCODE,
               rec_nfac.ReinsCode,
               rec_plan.plandate,
               rec_nfac.Ricurr,
               rec_nfac.netPREM * rec_plan.planrate,
               null
          from dual;
    end loop;
  end loop;

  --ÿ����ڸ������,����ÿ���ٱ���ĳһ�ڱ��ϡ�
  for rec_sumPlan in cur_sumPlan Loop
    select NETPREM
      into v_SumPrem
      from REINS_REPLY_N_FAC_REINS
     where repolicyno = p_repolicyno
       and rectimes = p_rectimes
       and layerno = rec_sumplan.layerno
       and reinscode = rec_sumPlan.reinscode
       and nvl(brokercode, '*') = nvl(rec_sumPlan.brokerCode, '*');

    if abs(v_SumPrem - rec_sumPlan.planfee) >= 0.01 then
      update REINS_REPLY_N_FAC_PLAN
         set planfee = planfee + v_SumPrem - rec_sumPlan.planfee
       where repolicyno = p_repolicyno
         and rectimes = p_rectimes
         and layerno = rec_sumplan.layerno
         and reinscode = rec_sumPlan.reinscode
         and nvl(brokercode, '*') = nvl(rec_sumPlan.brokerCode, '*')
         and rownum = 1;
    end if;
  end loop;

  p_message_code := '0';
  p_message_desc := '����ɹ�';
  update REINS_REPOLICY
     set status = '1', caldate = sysdate,date_updated=sysdate
   where repolicyno = p_repolicyno
     and rectimes = p_rectimes;
  commit;

exception
  when others then
    if v_error_Code = '01' then
      p_message_code := '0';
      p_message_desc := '�˷ֱ����Ѽ���������';
    else
      p_message_code := '1';
      p_message_desc := '����ʧ��';
    end if;

    --���������־
    g_errmsg:=g_errmsg||'-'||substr(sqlerrm,1,100);
    rollback;
    update REINS_REPOLICY set status = '2',date_updated=sysdate
       where repolicyno = p_repolicyno and rectimes = p_rectimes;
    insert into REINS_POLICY_ERR_LOG(errtype,repolicyno,rectimes,errmsg)
          values('B1',p_repolicyno,p_rectimes,g_errmsg);
    commit;
end nfac_ply_cal;

/* �Ǳ����ٷַ��������� */
procedure nfac_edr_cal(p_repolicyno in REINS_REENDOR.repolicyno%type,
                      p_reendortimes in REINS_REENDOR.reendortimes%type,
                      p_RecTimes   in REINS_REENDOR.RecTimes%type,
                      p_message_code out varchar2,
                      p_message_desc out varchar2 ) is

  cursor cur_tty(p_ply_year number, p_ttytype REINS_TTY_TABLE.ttytype%type) is
    select a.ttyid, b.StatClass, b.TtyType, a.ttycode, a.Uwyear
      from REINS_TREATY a, REINS_TTY_TABLE b
     where a.Uwyear = p_ply_year
       and a.ttycode = b.ttycode
       and b.ttytype = p_ttytype
       ORDER BY a.ttyid;

  cursor cur_edr is
    select *
      from REINS_REENDOR
     where repolicyno = p_repolicyno
       and reendortimes = p_reendortimes
       and RecTimes = p_Rectimes
       and Status in ('0', '2')
       for update;
  rec_edr cur_edr%rowtype;

  --���±��������ٷֳַ����
  cursor cur_edr_share is
    select b.layerno, b.BrokerCode, b.ReinsCode, --b.paycode, b.agentcode,
           a.RICurr, a.ExchRate, b.ShareRate, b.RIPrem,
           b.reinscommamount reinscomm,a.ricomm layerricomm--add by wangmx 20160806
      from REINS_REENDR_N_FAC a, REINS_REENDR_N_FAC_REINS b
     where a.repolicyno = b.repolicyno
       and a.reendortimes = b.reendortimes
       and a.RecTimes = b.RecTimes
       and a.Layerno = b.Layerno
       and a.repolicyno = p_repolicyno
       and a.reendortimes = p_reendortimes
       and a.RecTimes = p_Rectimes;
  rec_edr_share cur_edr_share%rowType;

  --�ϴγ����ٷֱַ����
  cursor cur_last_re(p_last_reendortimes REINS_REENDOR.reendortimes%type) is
     select b.layerno, b.BrokerCode, b.ReinsCode,--b.paycode,b.agentcode,
            a.RICurr, a.ExchRate, b.ShareRate, b.RIPrem,a.premium layerpremium,
            b.reinscommamount reinscomm,a.ricomm layerricomm--add by wangmx 20160806
     from REINS_REPLY_N_FAC a, REINS_REPLY_N_FAC_REINS b
    where a.RepolicyNo = b.RepolicyNo
      and a.RecTimes = b.RecTimes
      and a.layerno = b.layerno
      and a.RepolicyNo = p_repolicyno
      and p_last_reendortimes = '000'
  union
     select b.layerno, b.BrokerCode, b.ReinsCode, --b.paycode, b.agentcode,
            a.RICurr, a.ExchRate, b.ShareRate, b.RIPrem,a.premium layerpremium,
            b.reinscommamount reinscomm,a.ricomm layerricomm--add by wangmx 20160806
     from REINS_REENDR_N_FAC a, REINS_REENDR_N_FAC_REINS b
    where a.RepolicyNo = b.RepolicyNo
      and a.reendortimes = b.reendortimes
      and a.RecTimes = b.RecTimes
      and a.layerno = b.layerno
      and a.RepolicyNo = p_repolicyno
      and a.Reendortimes = p_last_reendortimes
      and p_last_reendortimes <> '000';
   rec_last_re  cur_last_re%rowtype;

  --���ڼƻ�
  cursor cur_plan(v_INSTMARK REINS_REPOLICY.INSTMARK%type,v_chgGrsPrem REINS_REPOLICY.GrsPrem%type,v_EndorTimes REINS_REPOLICY.Endortimes%type)is
     select 1 PayNo,rec_edr.validdate PlanDate,1 planrate
       from dual
      where v_INSTMARK='N'
    union
     select payno,PlanDate, decode(v_chgGrsPrem,0,1,ChgGRSPLANFEE/v_chgGrsPrem) planrate
       from REINS_ENDOR_UNIT_PLAN
      where v_INSTMARK='Y'
        and PolicyNo=rec_edr.PolicyNo
        and EndorTimes=rec_edr.EndorTimes
        and DangerUnitNo=rec_edr.DangerUnitNo
        and ReRiskCode=rec_edr.ReRiskCode
        and RiskCode=rec_edr.RiskCode
        and v_endortimes<>'000'
    union
     select payno,PlanDate,GRSPLANFEE/v_chgGrsPrem  planrate
       from REINS_POLICY_UNIT_PLAN
      where v_INSTMARK='Y'
        and PolicyNo=rec_edr.PolicyNo
        and DangerUnitNo=rec_edr.DangerUnitNo
        and ReRiskCode=rec_edr.ReRiskCode
        and RiskCode=rec_edr.RiskCode
        and v_endortimes='000';
  rec_plan cur_plan%rowtype;

  cursor cur_nfac is
    select a.LAYERNO, b.BROKERCODE, b.ReinsCode, a.Ricurr, b.ChgRiPrem,
              b.chgricomm--add by wangmx 20160806
      from REINS_REENDR_N_FAC a, REINS_REENDR_N_FAC_REINS b
     where a.repolicyno = p_repolicyno
       and a.reendortimes = p_reendortimes
       and a.RecTimes = p_Rectimes
       and a.repolicyno = b.repolicyno
       and a.reendortimes = b.reendortimes
       and a.RecTimes = b.RecTimes
       and a.Layerno = b.Layerno;
  rec_nfac cur_nfac%rowtype;

  cursor cur_sumPlan is
    select layerno, reinscode, brokercode, sum(planfee) as planfee
      from REINS_REENDR_N_FAC_PLAN
     where repolicyno = p_repolicyno
       and reendortimes = p_reendortimes
       and rectimes = p_Rectimes
     group by layerno, reinscode, brokercode;
  rec_sumPlan cur_sumPlan%rowtype;

  v_sharerate   number;
  v_xlsharerate number;
  v_last_Reendortimes REINS_REPOLICY.Endortimes%type;
  v_Status      REINS_REPOLICY.status%type;

  v_RiPrem      REINS_REPOLICY.GRSPREM%type := 0 ;
  v_ChgRiPrem   REINS_REPOLICY.GRSPREM%type := 0 ;
  v_LeftPrem    REINS_REPOLICY.GRSPREM%type := 0 ;
  v_ChgLeftPrem REINS_REPOLICY.GRSPREM%type := 0 ;

  v_INSTMARK    REINS_REPOLICY.instmark%type;
  v_endortimes  REINS_REPOLICY.Endortimes%type;
  v_OriChgGrsPrem  REINS_REPOLICY.GRSPREM%type;
  v_SumPrem     REINS_REPOLICY.GRSPREM%type := 0 ; --�ٱ��˲�ֳ�����
  v_sum_loss number(16,2):=0; --�ܲ��޶�
  v_sum_riComm number(16,2):=0;--�������� add by wangmx 20160806
  v_ChgRiComm  REINS_REENDR_N_FAC.Chgricomm%type :=0;--add by wangmx 20160806
  v_RiComm        REINS_REENDR_N_FAC.Ricomm%type:=0;--add by wangmx 20160806
  type nfac_share is record (
    LayerNo        REINS_REENDR_N_FAC_REINS.LAYERNO%type,
    BrokerCode     REINS_REENDR_N_FAC_REINS.BrokerCode%type,
    ReinsCode      REINS_REENDR_N_FAC_REINS.ReinsCode%type,
    RICurr         REINS_REENDR_N_FAC.RiCurr%type,
    ExchRate       REINS_REENDR_N_FAC.ExchRate%type,
    ShareRate      REINS_REENDR_N_FAC_REINS.ShareRate%type,
    RiPrem         REINS_REENDR_N_FAC_REINS.RiPrem%type,
    ChgRiPrem      REINS_REENDR_N_FAC_REINS.ChgRiPrem%type,
    layerPremium   REINS_REENDR_N_FAC_REINS.ChgRiPrem%type,
    ChgRiComm      REINS_REENDR_N_FAC_REINS.Chgricomm%type,--add by wangmx 20160806
    RiComm            REINS_REENDR_N_FAC.Ricomm%type,--add by wangmx 20160806
    layerRiComm     REINS_REENDR_N_FAC.Ricomm%type--add by wangmx 20160806
    );

   Type Arr_nfac_share is table of nfac_share Index By BINARY_INTEGER;   --���ÿ�ηֱ��������
   v_last_arr_nfac_share    arr_nfac_share;
   v_arr_nfac_share         arr_nfac_share;
   v_arr_re_abs             arr_re_abs; --v_arr_re_abs���������Ϊ�˹��ò���ֱ������ķ���

   v_char        char(1); --�����쳣��ֹ
   v_error_code  varchar(2) := '00';
   n_cnt         number;
   n_seq1        number;
   n_seq2        number;
   v_chgflag     varchar2(1);
begin
  open cur_edr;
  fetch cur_edr into rec_edr;
  if cur_edr%notfound then
    v_error_Code := '01';
    close cur_edr;
    g_errcode:='B2301';
    g_errmsg:='�ٷֳ�������Ҳ���������';
    select '*' into v_char from dual where 1 = 2;
  end if;
  close cur_edr;

  v_xlsharerate := rec_edr.sharerate;
  g_errcode:='B2302';
  g_errmsg:='�ٷֳ���������ϴ��ٱ����Ĵ�������';
  --ȡ���ϴγ����ٷֱַ����
  v_last_Reendortimes := get_last_EndorTimes(rec_edr.repolicyno,rec_edr.reendortimes,rec_edr.restartdate);
  v_Status := get_last_Status(rec_edr.repolicyno,v_last_Reendortimes);
  if v_Status<>'1' then
      g_errcode:='B2303';
     g_errmsg:='�ٷֳ�������ҷ��������ϴηֱ�״̬����';
      select '*' into v_char from dual where 1=2;
  end if;

  update REINS_REENDR_N_FAC_REINS a
     set a.riprem =
         (select b.premium * a.sharerate/100
            from REINS_REENDR_N_FAC b
           where a.repolicyno = b.repolicyno
             and a.reendortimes = b.reendortimes
             and a.rectimes = b.rectimes
             and a.layerno = b.layerno)
   where a.repolicyno = p_repolicyno
     and a.reendortimes = p_reendortimes
     and a.rectimes = p_RecTimes;

  n_cnt := 1;
  for rec_last_re in cur_last_re(v_last_Reendortimes) loop
    v_last_arr_nfac_share(n_cnt).layerno := rec_last_re.layerno;
    v_last_arr_nfac_share(n_cnt).BrokerCode := rec_last_re.BrokerCode;
    v_last_arr_nfac_share(n_cnt).ReinsCode := rec_last_re.ReinsCode;
    v_last_arr_nfac_share(n_cnt).RICurr := rec_last_re.RICurr;
    v_last_arr_nfac_share(n_cnt).ExchRate := rec_last_re.ExchRate;
    v_last_arr_nfac_share(n_cnt).ShareRate := rec_last_re.ShareRate;
    v_last_arr_nfac_share(n_cnt).RIPrem := rec_last_re.RIPrem;
    v_last_arr_nfac_share(n_cnt).ChgRIPrem := rec_last_re.RIPrem;
    v_last_arr_nfac_share(n_cnt).layerpremium:=rec_last_re.layerpremium;
    --modify by wangmx 20160806 begin
    if  rec_last_re.reinscomm =0 or  rec_last_re.reinscomm is null then
      v_last_arr_nfac_share(n_cnt).ricomm:=0;
    else
      v_last_arr_nfac_share(n_cnt).ricomm:= rec_last_re.reinscomm;
    end if;
    v_last_arr_nfac_share(n_cnt).layerricomm :=rec_last_re.layerricomm;
    --modify by wangmx 20160806 end
    n_cnt := n_cnt+1;
  end loop;
  --�Ǳ����ٷֳ����������ԭ��������ռ�ȼ��㣬��Ϊ���ܲ��޶�ռ�ȼ���
  select sum((t.endloss - t.excessloss) / t.exchrate)
    into v_sum_loss
    from REINS_REendr_N_FAC t
   where t.repolicyno = p_repolicyno
     and t.reendortimes = p_reendortimes
     and t.rectimes = p_rectimes;
  if v_sum_loss=0 or v_sum_loss is null then
      v_sharerate := 0;
    else
      v_sharerate:=v_sum_loss/rec_edr.pml;
    end if;
  --ȡ�����±��������ٷֳַ����
  n_cnt := 1;
  g_errcode:='B2304';
  g_errmsg:='�ٷֳ�������ҷ������ı��ηֱ��������';
  for rec_edr_share in cur_edr_share loop
    v_arr_nfac_share(n_cnt).layerno := rec_edr_share.layerno;
    v_arr_nfac_share(n_cnt).BrokerCode := rec_edr_share.BrokerCode;
    v_arr_nfac_share(n_cnt).ReinsCode := rec_edr_share.ReinsCode;
    v_arr_nfac_share(n_cnt).RICurr := rec_edr_share.RICurr;
    v_arr_nfac_share(n_cnt).ExchRate := rec_edr_share.ExchRate;
    v_arr_nfac_share(n_cnt).ShareRate := rec_edr_share.ShareRate;
    v_arr_nfac_share(n_cnt).RIPrem := rec_edr_share.RIPrem;
    --modify by wangmx 20160806 begin
    if rec_edr_share.reinscomm =0 or rec_edr_share.reinscomm is null then
      v_arr_nfac_share(n_cnt).ricomm :=0;
    else
      v_arr_nfac_share(n_cnt).ricomm := rec_edr_share.reinscomm;--add by wangmx 20160806
    end if;
    ----modify by wangmx 20160806 end
    n_cnt := n_cnt+1;
  end loop;

  --��ñ��������仯��
  for n_seq1 in 1..v_arr_nfac_share.count loop
    v_chgflag := 0;
    for n_seq2 in 1..v_last_arr_nfac_share.count loop
      if v_last_arr_nfac_share(n_seq2).layerno=v_arr_nfac_share(n_seq1).layerno
         and nvl(v_last_arr_nfac_share(n_seq2).BrokerCode,'*')=nvl(v_arr_nfac_share(n_seq1).BrokerCode,'*')
         and v_last_arr_nfac_share(n_seq2).ReinsCode=v_arr_nfac_share(n_seq1).ReinsCode
        then
           v_arr_nfac_share(n_seq1).ChgRIPrem := v_arr_nfac_share(n_seq1).RIPrem - v_last_arr_nfac_share(n_seq2).RIPrem;
           v_arr_nfac_share(n_seq1).chgricomm := v_arr_nfac_share(n_seq1).ricomm - v_last_arr_nfac_share(n_seq2).ricomm;--add by wangmx 20160806
           v_chgflag := '1';
           update REINS_REENDR_N_FAC a
              set a.chgprem = a.premium - v_last_arr_nfac_share(n_seq2).layerpremium
            where a.repolicyno = p_repolicyno
              and a.reendortimes = p_reendortimes
              and a.rectimes = p_RecTimes
              and a.layerno = v_arr_nfac_share(n_seq1).layerno;
         /* v_RiComm := v_RiComm + v_arr_nfac_share(n_seq1).ricomm;
          v_ChgRiComm :=v_ChgRiComm + v_arr_nfac_share(n_seq1).chgricomm;*/
      end if;
      --modify by wangmx 20160806 begin
      update REINS_REENDR_N_FAC a
        set a.ricomm = (select sum(b.reinscommamount) from Reins_Reendr_n_Fac_Reins b where a.rectimes=b.rectimes and a.repolicyno=b.repolicyno and a.reendortimes=b.reendortimes and a.layerno=b.layerno) ,
             a.chgricomm = (select sum(b.chgricomm) from Reins_Reendr_n_Fac_Reins b where a.rectimes=b.rectimes and a.repolicyno=b.repolicyno and a.reendortimes=b.reendortimes and a.layerno=b.layerno)
       where a.repolicyno = p_repolicyno
              and a.reendortimes = p_reendortimes
              and a.rectimes = p_RecTimes
              and a.layerno = v_arr_nfac_share(n_seq1).layerno;
        --modify by wangmx 20160806 end
    end loop;
    if v_chgflag='0' then
      v_arr_nfac_share(n_seq1).ChgRIPrem := v_arr_nfac_share(n_seq1).RIPrem;
      update REINS_REENDR_N_FAC a
         set a.chgprem = a.premium
       where a.repolicyno = p_repolicyno
         and a.reendortimes = p_reendortimes
         and a.rectimes = p_RecTimes
         and a.layerno = v_arr_nfac_share(n_seq1).layerno;
    end if;
    --modify by wangmx 20160806 begin
    if v_chgflag='0' then
      v_arr_nfac_share(n_seq1).chgricomm := v_arr_nfac_share(n_seq1).RiComm;
      update REINS_REENDR_N_FAC a
         set a.chgricomm = a.ricomm
       where a.repolicyno = p_repolicyno
         and a.reendortimes = p_reendortimes
         and a.rectimes = p_RecTimes
         and a.layerno = v_arr_nfac_share(n_seq1).layerno;
    end if;
     --modify by wangmx 20160806 end
  end loop;

  --�������������ٷ��ٱ�����Ϣ����
  for n_seq1 in 1..v_arr_nfac_share.count loop
   update REINS_REENDR_N_FAC_REINS
       set ChgRIPrem = v_arr_nfac_share(n_seq1).ChgRIPrem,
            chgricomm = v_arr_nfac_share(n_seq1).chgricomm
     where RePolicyNo=p_RePolicyNo
       and reendortimes=p_reendortimes
       and RecTimes=p_Rectimes
       and Layerno=v_arr_nfac_share(n_seq1).Layerno
       and ReinsCode=v_arr_nfac_share(n_seq1).ReinsCode
       and nvl(BrokerCode,'*')=nvl(v_arr_nfac_share(n_seq1).BrokerCode,'*');
       --�ֳܷ������Էֳ����ѱ仯��
       v_RiPrem := v_RiPrem + v_arr_nfac_share(n_seq1).RIPrem;
       v_ChgRiPrem := v_ChgRiPrem + v_arr_nfac_share(n_seq1).ChgRIPrem;
       --��
       v_RiComm :=v_RiComm + v_arr_nfac_share(n_seq1).ricomm;
       v_ChgRiComm := v_ChgRiComm + v_arr_nfac_share(n_seq1).chgRicomm;
  end loop;

  g_errcode:='B2305';
  g_errmsg:='�ٷֳ�����㳬���ٷּ������';

  -----------�����ٷּ���----------------��
  v_arr_re_abs(1).PCind := 'P';
  v_arr_re_abs(1).NetInd := '0';
  if v_arr_re_abs(1).NetInd = '1' then
    v_arr_re_abs(1).RIPrem := v_RiPrem;
    v_arr_re_abs(1).NetPrem := v_RiPrem;
    v_arr_re_abs(1).ChgRIPrem := v_ChgRiPrem;
    v_arr_re_abs(1).ChgNetPrem := v_ChgRiPrem;

    select rec_edr.pml * v_sharerate,
           v_RiPrem * decode(rec_edr.NetPrem,0,0,rec_edr.GrsPrem/rec_edr.NetPrem),
           v_ChgRiPrem * decode(rec_edr.ChgNetPrem,0,0,rec_edr.ChgGrsPrem/rec_edr.ChgNetPrem)/*,
           decode(rec_edr.ChgNetPrem,0,0,v_ChgRiPrem/rec_edr.ChgNetPrem)*/
      into v_arr_re_abs(1).RISum,
           v_arr_re_abs(1).GrsPrem,
           v_arr_re_abs(1).ChgGrsPrem/*,
           v_ShareRate*/
      from dual;
  else
    v_arr_re_abs(1).RIPrem := v_RiPrem;
    v_arr_re_abs(1).GrsPrem := v_RiPrem;
    v_arr_re_abs(1).ChgRIPrem := v_ChgRiPrem;
    v_arr_re_abs(1).ChgGrsPrem := v_ChgRiPrem;

    select rec_edr.pml * v_sharerate,
           v_RiPrem * decode(rec_edr.GrsPrem,0,0,rec_edr.NetPrem/rec_edr.GrsPrem),
           v_ChgRiPrem * decode(rec_edr.ChgGrsPrem,0,0,rec_edr.ChgNetPrem/rec_edr.ChgGrsPrem)/*,
           decode(rec_edr.ChgGrsPrem,0,0,v_ChgRiPrem/rec_edr.ChgGrsPrem)*/
      into v_arr_re_abs(1).RISum,
           v_arr_re_abs(1).NetPrem,
           v_arr_re_abs(1).ChgNetPrem/*,
           v_ShareRate*/
      from dual;
  end if;
  v_arr_re_abs(1).Chgrisum := rec_edr.ChgPml * v_ShareRate;
  v_arr_re_abs(1).GEARNEDPREM := 0;
  v_arr_re_abs(1).GPORTFPREM := 0;
  v_arr_re_abs(1).NEARNEDPREM := 0;
  v_arr_re_abs(1).NPORTFPREM := 0;

  v_arr_re_abs(1).SectNo    := '0';
  v_arr_re_abs(1).ShareRate := v_ShareRate * 100;
  v_arr_re_abs(1).RICurr    := rec_edr.currency;
  v_arr_re_abs(1).ExchRate  := 1;
  --û��Ӷ��
  v_arr_re_abs(1).ricomm    := 0;
  v_arr_re_abs(1).chgRIComm := 0;

  open cur_tty(to_number(to_char(rec_edr.startdate, 'yyyy')), '92'); --�����ٷֺ�Լ��Ҫ�ġ�
  fetch cur_tty
    into v_arr_re_abs(1).ttyid, v_arr_re_abs(1).StatClass, v_arr_re_abs(1).TtyType, v_arr_re_abs(1).ttycode, v_arr_re_abs(1).Uwyear;
  close cur_tty;

  --modify by liupeng 20160518
  --������������ܲ��޶��������������
  -----------�����ٷֵĲ��ַŵ�����������Լ------------
  /*if v_arr_re_abs(1).NetInd = '1' then
    v_ChgLeftPrem := rec_edr.ChgNetPrem * v_xlsharerate / 100 - v_arr_re_abs(1).ChgNetPrem ;
    v_LeftPrem := rec_edr.NetPrem * v_xlsharerate / 100 - v_arr_re_abs(1).NetPrem ;
  else
    v_ChgLeftPrem := rec_edr.ChgGrsPrem * v_xlsharerate / 100 - v_arr_re_abs(1).ChgGrsPrem ;
    v_LeftPrem := rec_edr.GrsPrem * v_xlsharerate / 100 - v_arr_re_abs(1).GrsPrem ;
  end if;
  if abs(v_ChgLeftPrem) >= 0.01 or abs(v_LeftPrem) >= 0.01 then
    v_arr_re_abs(2).PCind := v_arr_re_abs(1).PCind;
    v_arr_re_abs(2).NetInd := v_arr_re_abs(1).NetInd;

    v_arr_re_abs(2).RISum := rec_edr.Pml * v_xlsharerate / 100 - v_arr_re_abs(1).RISum ;
    v_arr_re_abs(2).RIPrem := v_LeftPrem;
    v_arr_re_abs(2).GrsPrem := rec_edr.GrsPrem * v_xlsharerate / 100 - v_arr_re_abs(1).GrsPrem ;
    v_arr_re_abs(2).NetPrem := rec_edr.NetPrem * v_xlsharerate / 100 - v_arr_re_abs(1).NetPrem ;

    v_arr_re_abs(2).ChgRISum := rec_edr.Pml * v_xlsharerate / 100 - v_arr_re_abs(1).ChgRISum ;
    v_arr_re_abs(2).ChgRIPrem := v_ChgLeftPrem;
    v_arr_re_abs(2).ChgGrsPrem := rec_edr.ChgGrsPrem * v_xlsharerate / 100 - v_arr_re_abs(1).ChgGrsPrem ;
    v_arr_re_abs(2).ChgNetPrem := rec_edr.ChgNetPrem * v_xlsharerate / 100 - v_arr_re_abs(1).ChgNetPrem ;

    v_arr_re_abs(2).GEARNEDPREM := 0;
    v_arr_re_abs(2).GPORTFPREM := 0;
    v_arr_re_abs(2).NEARNEDPREM := 0;
    v_arr_re_abs(2).NPORTFPREM := 0;

    v_arr_re_abs(2).SectNo := '0';
    v_arr_re_abs(2).ShareRate := v_xlsharerate - v_arr_re_abs(1).ShareRate ;
    v_arr_re_abs(2).RICurr := v_arr_re_abs(1).RICurr;
    v_arr_re_abs(2).ExchRate := 1;
    --û��Ӷ��
    v_arr_re_abs(2).ricomm    := 0;
    v_arr_re_abs(2).chgRIComm := 0;

    open cur_tty(to_number(to_char(rec_edr.startdate, 'yyyy')), '82'); --����������Լ��
    fetch cur_tty
      into v_arr_re_abs(2).ttyid, v_arr_re_abs(2).StatClass, v_arr_re_abs(2).TtyType, v_arr_re_abs(2).ttycode, v_arr_re_abs(2).Uwyear;
    close cur_tty;
  end if;*/
  g_errcode:='B2306';
  g_errmsg:='�ٷֳ���������ɷ������ֱ����';
  --���ɷ������ֱ����
  crt_edr_abs(p_repolicyno, p_reendortimes, p_RecTimes, v_Arr_Re_Abs);

  g_errcode:='B2307';
  g_errmsg:='�ٷֳ���������ɳ����ٷַ��ڼƻ���';
  -----------�����ٷַ��ڼƻ���------------
  if rec_edr.ChgGrsPrem=0 then
   v_INSTMARK:='N';
  else
   v_INSTMARK:=rec_edr.INSTMARK;
  end if;
  v_endortimes := lpad(nvl(rec_edr.endortimes,'000'),3,'0');
  /*select lpad(nvl(endortimes,'000'),3,'0') into v_endortimes
    from REINS_REENDOR where repolicyno=p_repolicyno and rectimes=p_rectimes and reendortimes=p_reendortimes;*/
  --ȡΣ�յ�λë����
  g_errmsg:='ȡΣ�յ�λë����';
  if v_endortimes<>'000' then
    select nvl(Chggrsprem,0) into v_OriChgGrsPrem
     from REINS_ENDOR_UNIT
    where PolicyNo=rec_edr.PolicyNo
      and EndorTimes=rec_edr.EndorTimes
      and DangerUnitNo=rec_edr.DangerUnitNo
      and ReRiskCode=rec_edr.ReRiskCode
      and RiskCode=rec_edr.RiskCode;
  else
    select GrsPrem into v_OriChgGrsPrem
      from REINS_POLICY_UNIT
     where PolicyNo=rec_edr.PolicyNo
       and DangerUnitNo=rec_edr.DangerUnitNo
       and ReRiskCode=rec_edr.ReRiskCode
       and RiskCode=rec_edr.RiskCode;
  end if;

   open cur_plan(v_INSTMARK,v_OriChgGrsPrem,v_endortimes);
   fetch cur_plan into rec_plan;
      if cur_plan%notfound then
         g_errmsg:='����û�з�����Ϣ';
         close cur_plan;
         select '*' into v_char from dual where 1=2;
      end if;
   close cur_plan;

  for rec_plan in cur_plan(v_INSTMARK,v_OriChgGrsPrem,v_endortimes) loop
    for rec_nfac in cur_nfac loop
      insert into REINS_REENDR_N_FAC_PLAN
        (REPOLICYNO,
         RECTIMES,
         REENDORTIMES,
         LAYERNO,
         PAYNO,
         BROKERCODE,
         REINSCODE,
         PAYDATE,
         CURRENCY,
         PLANFEE,
         REMARKS)
        select p_repolicyno,
               p_rectimes,
               p_reendortimes,
               rec_nfac.LAYERNO,
               rec_plan.PAYNO,
               rec_nfac.BROKERCODE,
               rec_nfac.ReinsCode,
               rec_plan.plandate,
               rec_nfac.Ricurr,
               rec_nfac.ChgRiPrem * rec_plan.planrate,
               null
          from dual;
    end loop;
  end loop;

  --ÿ����ڸ������,����ÿ���ٱ���ĳһ�ڱ��ϡ�
  for rec_sumPlan in cur_sumPlan Loop
    select ChgRiPrem
      into v_SumPrem
      from REINS_REENDR_N_FAC_REINS
     where repolicyno = p_repolicyno
       and reendortimes = p_reendortimes
       and rectimes = p_rectimes
       and layerno = rec_sumplan.layerno
       and reinscode = rec_sumPlan.reinscode
       and nvl(brokercode, '*') = nvl(rec_sumPlan.brokerCode, '*');

    if abs(v_SumPrem - rec_sumPlan.planfee) >= 0.01 then
      update REINS_REENDR_N_FAC_PLAN
         set planfee = planfee + v_SumPrem - rec_sumPlan.planfee
       where repolicyno = p_repolicyno
         and reendortimes = p_reendortimes
         and rectimes = p_rectimes
         and layerno = rec_sumplan.layerno
         and reinscode = rec_sumPlan.reinscode
         and nvl(brokercode, '*') = nvl(rec_sumPlan.brokerCode, '*')
         and rownum = 1;
    end if;
  end loop;

  p_message_code := '0';
  p_message_desc := '����ɹ�';
  update REINS_REENDOR
     set status = '1', caldate = sysdate,date_updated=sysdate
   where repolicyno = p_repolicyno
     and rectimes = p_rectimes
     and reendortimes = p_reendortimes;
  commit;

  exception when others then
    if v_error_Code = '01' then
      p_message_code := '0';
      p_message_desc := '�˷������Ѽ���������';
    else
      p_message_code := '1';
      p_message_desc := '����ʧ��';
    end if;

    --��¼������־��Ϣ
    g_errmsg:=g_errmsg||'-'||substr(sqlerrm,1,100);
    rollback;
    update REINS_REENDOR set status = '2',date_updated=sysdate
       where repolicyno = p_repolicyno
         and reendortimes = p_reendortimes
         and rectimes = p_rectimes;
    insert into REINS_ENDOR_ERR_LOG(errtype,repolicyno,reendortimes,rectimes,errmsg)
       values('B2',p_repolicyno,p_reendortimes,p_rectimes,g_errmsg);
    commit;
end nfac_edr_cal;

/* �Ǳ����ٷ��Ѿ����ⰸ���� */
procedure nfac_clm_cal(p_reclaimno in REINS_RECLAIM.reclaimno%type,
                      p_RecTimes  in REINS_RECLAIM.RecTimes%type,
                      p_message_code out varchar2,
                      p_message_desc out varchar2 ) is
cursor cur_tty(p_ply_year number, p_ttytype REINS_TTY_TABLE.ttytype%type) is
  select a.ttyid,b.StatClass,b.TtyType,a.ttycode,a.Uwyear
    from REINS_TREATY a,REINS_TTY_TABLE b
   where a.Uwyear=p_ply_year
     and a.ttycode=b.ttycode
     and b.ttytype=p_ttytype
     ORDER BY a.ttyid;
rec_tty cur_tty%rowtype;

cursor cur_clm is
  select * from REINS_RECLAIM
   where reclaimno=p_reclaimno
     and RecTimes=p_Rectimes
     and Status in ('0','2') for update;
rec_clm cur_clm%rowtype;

v_reclm_para  reclm_para;
cursor cur_reins_share is
   select distinct a.Layerno, c.BROKERCODE, c.REINSCODE, c.PAYCODE, c.AGENTCODE, c.SHARERATE
     from REINS_REPLY_N_FAC a, REINS_REPLY_N_FAC_REINS c, REINS_REPOLICY b
    where b.policyNo=v_reclm_para.policyno
      and b.DANGERUNITNO=v_reclm_para.DANGERUNITNO
      and b.riskcode=v_reclm_para.riskcode
      and b.reriskcode=v_reclm_para.reriskcode
      and b.restartdate<=v_reclm_para.DamageDATE
      and b.reenddate>=v_reclm_para.DamageDATE
      and b.Status='1'
      and b.reinstype='2'
      and a.RepolicyNo=b.RepolicyNo
      and a.RecTimes=b.RecTimes
      and a.RepolicyNo=c.RepolicyNo
      and a.RecTimes=c.RecTimes
      and a.Layerno=c.Layerno
  union all
   select distinct a.Layerno, c.BROKERCODE, c.REINSCODE, c.PAYCODE, c.AGENTCODE, c.SHARERATE
     from REINS_REENDR_N_FAC a, REINS_REENDR_N_FAC_REINS c, REINS_REENDOR b
    where b.policyNo=v_reclm_para.policyno
      and b.DANGERUNITNO=v_reclm_para.DANGERUNITNO
      and b.riskcode=v_reclm_para.riskcode
      and b.reriskcode=v_reclm_para.reriskcode
      and b.restartdate<=v_reclm_para.DamageDATE
      and b.reenddate>=v_reclm_para.DamageDATE
      and b.Status='1'
      and b.reinstype='2'
      and a.RepolicyNo=b.RepolicyNo
      and a.RecTimes=b.RecTimes
      and a.reendortimes=b.reendortimes
      and a.RepolicyNo=c.RepolicyNo
      and a.RecTimes=c.RecTimes
      and a.reendortimes=c.reendortimes
      and a.Layerno=c.Layerno;
rec_reins_share  cur_reins_share%rowtype;

cursor cur_clmnfac is
  select sum(CHGPAIDSUM) CHGPAIDSUM, sum(paidSum) paidSum
   from REINS_RECLM_N_FAC
   where reclaimno=p_reclaimno
     and RecTimes=p_Rectimes;
rec_clmnfac cur_clmnfac%rowtype;

cursor cur_paidsum is
    select layerno, sum(paidsum) paidsum, sum(chgpaidsum) chgpaidsum
      from REINS_RECLM_N_FAC_REINS
     where reclaimno=p_reclaimno
       and RecTimes=p_Rectimes
    group by layerno;
rec_paidsum   cur_paidsum%rowtype;

v_nfac_share  Arr_danger_nfac_share;
v_share       Arr_danger_share;
v_char        char(1); --�����쳣��ֹ
n_seq         number(3):=0;
v_xlsharerate REINS_REPOLICY.sharerate%type;
v_xlPaidSum   REINS_RECLM_N_FAC.paidsum%type:=0;
/*v_LeftPaidSum REINS_RECLM_N_FAC.paidsum%type:=0;*/
v_PaidSum     REINS_RECLM_N_FAC.paidsum%type:=0;
v_ChgPaidSum  REINS_RECLM_N_FAC.paidsum%type:=0;
v_error_code varchar(2):='00';
v_ncal_count number(2);
begin
   open cur_clm;
     fetch cur_clm into rec_clm;
     if cur_clm%notfound then
      g_errcode:='B3301';
      g_errmsg:='�ٷֳ����Ѿ����ⰸ�����Ҳ������ⰸ��Ϣ';
      select '*' into v_char from dual where 1=2;
     end if;
   close cur_clm;

   v_reclm_para.claimno:=rec_clm.claimno;
   v_reclm_para.RepolicyNo:=rec_clm.repolicyno;
   v_reclm_para.policyNo:=rec_clm.policyno;
   v_reclm_para.riskcode:=rec_clm.riskcode;
   v_reclm_para.reriskcode:=rec_clm.reriskcode;
   v_reclm_para.DANGERUNITNO:=rec_clm.DANGERUNITNO;
   v_reclm_para.Uwyear:=to_number(to_char(rec_clm.startdate,'yyyy'));
   v_reclm_para.Dangertype:=rec_clm.dangertype;
   v_reclm_para.DangerCode:=rec_clm.dangerCode;
   v_reclm_para.DamageDATE:=trunc(rec_clm.damagedate);
   v_reclm_para.currency:=rec_clm.currency;
   v_reclm_para.paidsum:=rec_clm.paidsum;
   --�����Ƿ����δ�����������ĺ�Լ���ⰸ
   select count(*)
     into v_ncal_count
     from reins_reclaim t
    where t.claimno = rec_clm.claimno
      and t.dangerunitno = rec_clm.dangerunitno
      and t.reriskcode = rec_clm.reriskcode
      and t.riskcode = rec_clm.riskcode
      and t.status in ('0','2','5')
      and t.reinstype = '0';
   --���ڷǱ����ٷ���أ��ǻ��ں�Լ��̯����������
   --����ǰ�����뱣֤��Լ���ⰸ�����������㣬������ڼ�������δ����ĺ�Լ���ⰸ���״�
   if v_ncal_count>0 then
      g_errcode:='B3399';
      g_errmsg:='����δ�����������ĺ�Լ���ⰸ';
      select '*' into v_char from dual where 1=2;
   end if;

   --����������ڴ��ڱ���ֹ�ڣ�����ڱ���ֹ��
  if rec_clm.DamageDATE>rec_clm.enddate then
     v_reclm_para.DamageDATE:=rec_clm.enddate;
  end if;
  --�����������С�ڱ������ڣ�����ڱ�������
  if rec_clm.DamageDATE<rec_clm.startDate then
     v_reclm_para.DamageDATE:=rec_clm.startDate;
  end if;
   g_errcode:='B3302';
   g_errmsg:='�ٷֳ����Ѿ����ⰸ����ȡ�ֳ�����';
   get_nfac_ply_share(v_reclm_para, v_nfac_share, v_xlsharerate);
   g_errcode:='B3303';
   g_errmsg:='�ٷֳ����Ѿ����ⰸ�����γ��ٷֲַ�����';
   if v_nfac_share.count >= 1 then
      --�Ѿ����ⰸ�Ǳ����ٷֲַ�����
      for n_seq in 1..v_nfac_share.count loop
         insert into REINS_RECLM_N_FAC(
             RECLAIMNO   ,
             RECTIMES    ,
             LAYERNO     ,
             CLAIMNO  ,
             POLICYNO  ,
             DANGERUNITNO   ,
             REPORTDATE     ,
             CURRENCY   ,
             PAIDSUM    ,
             --RENPREM,
             CHGPAIDSUM   ,
             --CHGRENPREM      ,
             GENBILLNO  )
          select
             p_reclaimno,
             p_Rectimes,
             v_nfac_share(n_seq).LAYERNO,
             rec_clm.CLAIMNO,
             rec_clm.POLICYNO ,
             rec_clm.DANGERUNITNO   ,
             grbilldeal.get_bill_date(rec_clm.uwenddate),
             rec_clm.currency,
             v_nfac_share(n_seq).PAIDSUM / v_nfac_share(n_seq).exchrate,
             --v_nfac_share(n_seq).RENPREM / v_nfac_share(n_seq).exchrate,
             v_nfac_share(n_seq).CHGPAIDSUM / v_nfac_share(n_seq).exchrate,
             --v_nfac_share(n_seq).CHGRENPREM / v_nfac_share(n_seq).exchrate,
             null
           from dual;
      end loop;
     g_errcode:='B3304';
     g_errmsg:='�ٷֳ����Ѿ����ⰸ�����γ��ٷ��ٱ��˱�';
      --�Ѿ����ⰸ�Ǳ����ٷ��ٱ��˱�
      for rec_reins_share in cur_reins_share loop
         insert into REINS_RECLM_N_FAC_REINS (
            RECLAIMNO,
            RECTIMES,
            LAYERNO,
            BROKERCODE,
            REINSCODE,
            PAYCODE,
            AGENTCODE,
            SHARERATE,
            PAIDSUM,
            CHGPAIDSUM,
            BILLNO,
            FLAG
            )
         select RECLAIMNO,
                RECTIMES,
                LAYERNO,
                rec_reins_share.BROKERCODE,
                rec_reins_share.REINSCODE,
                rec_reins_share.PAYCODE,
                rec_reins_share.AGENTCODE,
                rec_reins_share.SHARERATE,
                PAIDSUM * rec_reins_share.SHARERATE /100, --modify by liupeng �������*�ֳ�������Ӧ�ó�100
                CHGPAIDSUM * rec_reins_share.SHARERATE /100,
                null,
                null
            from REINS_RECLM_N_FAC
           where RECLAIMNO = p_reclaimno
             and RECTIMES = p_Rectimes
             and LAYERNO = rec_reins_share.Layerno;
      end loop;
      --�ֲ��ٱ��˵����
      for rec_paidsum in cur_paidsum loop
        select sum(paidsum), sum(chgpaidsum)
         into v_paidsum, v_chgpaidsum
         from REINS_RECLM_N_FAC
         where RECLAIMNO = p_reclaimno
           and RECTIMES = p_Rectimes
           and LAYERNO = rec_paidsum.layerno;

        if abs(v_paidsum-rec_paidsum.paidsum)>=0.01 or abs(v_chgpaidsum-rec_paidsum.chgpaidsum)>=0.01 then
           update REINS_RECLM_N_FAC_REINS set paidsum=paidsum + v_paidsum-rec_paidsum.paidsum,
                                       chgpaidsum=chgpaidsum + v_chgpaidsum-rec_paidsum.chgpaidsum
           where RECLAIMNO =p_reclaimno
             and RECTIMES = p_Rectimes
             and LAYERNO = rec_paidsum.layerno
             and rownum = 1;
        end if;
      end loop;
   end if;
   g_errcode:='B3305';
   g_errmsg:='�ٷֳ����Ѿ����ⰸ����дREINS_RECLAIM_SHARE�ֳ���';
   --�����ٷֳַ����� -->дREINS_RECLAIM_SHARE�ֳ���
   open cur_tty(to_number(to_char(rec_clm.startdate,'yyyy')), '92');--�����ٷֺ�Լ��Ҫ�ġ�
    fetch cur_tty into  rec_tty;
     for rec_clmnfac in cur_clmnfac loop
       n_seq:=n_seq+1;
       v_share(n_Seq).ttyid := rec_tty.ttyid;
       v_share(n_Seq).sectno := '0';
       v_share(n_seq).PCInd := 'P';
       v_share(n_seq).SHARERATE := rec_clmnfac.paidsum * 100 / rec_clm.PaidSum ;
       v_share(n_seq).RICURR := rec_clm.currency;
       v_share(n_seq).EXCHRATE := 1;
       v_share(n_seq).PAIDSUM := rec_clmnfac.chgpaidsum;
       v_xlPaidSum := v_xlPaidSum + v_share(n_seq).PAIDSUM;
     end loop;
   close cur_tty;

/*   --ʣ�ಿ�� �ŵ��������� -->дREINS_RECLAIM_SHARE�ֳ���
   v_LeftPaidSum := rec_clm.PaidSum * v_xlsharerate - v_xlPaidSum;
   if abs(v_LeftPaidSum)>=0.01 then
    open cur_tty(to_number(to_char(rec_clm.startdate,'yyyy')), '82');
     fetch cur_tty into  rec_tty;
      n_seq:=n_seq+1;
      v_share(n_Seq).ttyid := rec_tty.ttyid;
      v_share(n_Seq).sectno := '0';
      v_share(n_seq).PCInd := 'P';
      v_share(n_seq).SHARERATE := v_LeftPaidSum*100/(rec_clm.PaidSum * v_xlsharerate) ;
      v_share(n_seq).RICURR := rec_clm.currency;
      v_share(n_seq).EXCHRATE := 1;
      v_share(n_seq).PAIDSUM := v_LeftPaidSum;
    close cur_tty;
   end if;*/

  crt_clm_abs(p_reclaimno, p_RecTimes, v_share);
  g_errcode:='B3306';
  g_errmsg:='�ٷֳ����Ѿ����ⰸ�����д�Ļָ����ѺͲ����޶�';
  rewrite_nfac_ply(p_reclaimno, p_RecTimes, v_reclm_para);

  p_message_code :='0';
  p_message_desc:='����ɹ�';
  update REINS_RECLAIM set status='1',caldate=sysdate,date_updated=sysdate
   where reclaimno=p_reclaimno and rectimes=p_rectimes;
  commit;

  exception when others then
   if v_error_Code='01' then
      p_message_code:='0';
      p_message_desc:='�˷��ⰸ�ѽ��м���';
   else
      p_message_code:='1';
      p_message_desc:='����ʧ��';
   end if;

   g_errmsg:=g_errmsg||'-'|| substr(sqlerrm,1,100);
   rollback;
   update REINS_RECLAIM set status='2',date_updated=sysdate
     where reclaimno=p_reclaimno and rectimes=p_rectimes;
   insert into REINS_CLAIM_ERR_LOG(errtype,reclaimno,rectimes,errmsg)
         values('B3',p_reclaimno,p_rectimes,g_errmsg);
   commit;
end nfac_clm_cal;

/* �Ǳ����ٷ�δ�����ⰸ���� */
procedure nfac_OSclm_cal(p_osreclaimno in REINS_OS_RECLAIM.osreclaimno%type,
                        p_message_code out varchar2,
                        p_message_desc out varchar2 ) is
cursor cur_tty(p_ply_year number, p_ttytype REINS_TTY_TABLE.ttytype%type) is
  select a.ttyid,b.StatClass,b.TtyType,a.ttycode,a.Uwyear
    from REINS_TREATY a,REINS_TTY_TABLE b
   where a.Uwyear=p_ply_year
     and a.ttycode=b.ttycode
     and b.ttytype=p_ttytype
     ORDER BY a.ttyid;
rec_tty cur_tty%rowtype;

cursor cur_clm is
  select * from REINS_OS_RECLAIM
   where OSreclaimno=p_osreclaimno
     and Status in ('0','2') for update;
rec_clm cur_clm%rowtype;

v_reclm_para  reclm_para;
cursor cur_reins_share is
   select distinct a.Layerno, c.BROKERCODE, c.REINSCODE, c.PAYCODE, c.AGENTCODE, c.SHARERATE
     from REINS_REPLY_N_FAC a, REINS_REPLY_N_FAC_REINS c, REINS_REPOLICY b
    where b.policyNo=v_reclm_para.policyno
      and b.DANGERUNITNO=v_reclm_para.DANGERUNITNO
      and b.riskcode=v_reclm_para.riskcode
      and b.reriskcode=v_reclm_para.reriskcode
      and b.restartdate<=v_reclm_para.DamageDATE
      and b.reenddate>=v_reclm_para.DamageDATE
      and b.Status='1'
      and b.reinstype='2'
      and a.RepolicyNo=b.RepolicyNo
      and a.RecTimes=b.RecTimes
      and a.RepolicyNo=c.RepolicyNo
      and a.RecTimes=c.RecTimes
      and a.Layerno=c.Layerno
  union all
   select distinct a.Layerno, c.BROKERCODE, c.REINSCODE, c.PAYCODE, c.AGENTCODE, c.SHARERATE
     from REINS_REENDR_N_FAC a, REINS_REENDR_N_FAC_REINS c, REINS_REENDOR b
    where b.policyNo=v_reclm_para.policyno
      and b.DANGERUNITNO=v_reclm_para.DANGERUNITNO
      and b.riskcode=v_reclm_para.riskcode
      and b.reriskcode=v_reclm_para.reriskcode
      and b.restartdate<=v_reclm_para.DamageDATE
      and b.reenddate>=v_reclm_para.DamageDATE
      and b.Status='1'
      and b.reinstype='2'
      and a.RepolicyNo=b.RepolicyNo
      and a.RecTimes=b.RecTimes
      and a.reendortimes=b.reendortimes
      and a.RepolicyNo=c.RepolicyNo
      and a.RecTimes=c.RecTimes
      and a.reendortimes=c.reendortimes
      and a.Layerno=c.Layerno;
rec_reins_share  cur_reins_share%rowtype;

cursor cur_clmnfac is
  select sum(CHGOSSM) CHGOSSM, sum(OSSum) OSSum
   from REINS_OS_RECLM_N_FAC
   where osreclaimno=p_osreclaimno;
rec_clmnfac cur_clmnfac%rowtype;

cursor cur_ossum is
    select layerno, sum(ossum) ossum, sum(chgossum) chgossum
      from REINS_OS_RECLM_N_FAC_REINS
     where osreclaimno=p_osreclaimno
    group by layerno;
rec_ossum     cur_ossum%rowtype;

v_nfac_share  Arr_danger_nfac_share;
v_share       Arr_danger_share;
v_char        char(1); --�����쳣��ֹ
v_error_code  varchar(2):='00';
n_seq         number(3):=0;
v_xlsharerate REINS_REPOLICY.sharerate%type;
v_xlOSSum     REINS_OS_RECLM_N_FAC.ossum%type:=0;
/*v_LeftOSSum   REINS_OS_RECLM_N_FAC.ossum%type:=0;*/
v_OSSum       REINS_OS_RECLM_N_FAC.ossum%type:=0;
v_ChgOSSum    REINS_OS_RECLM_N_FAC.ossum%type:=0;
begin
   open cur_clm;
     fetch cur_clm into rec_clm;
     if cur_clm%notfound then
       v_error_Code:='01';
       g_errcode:='B4301';
       g_errmsg:='�ٷֳ���δ�������Ҳ������ⵥ��Ϣ';
       select '*' into v_char from dual where 1=2;
     end if;
   close cur_clm;

  v_reclm_para.claimno:=rec_clm.claimno;
  v_reclm_para.RepolicyNo:=rec_clm.repolicyno;
  v_reclm_para.policyNo:=rec_clm.policyno;
  v_reclm_para.riskcode:=rec_clm.riskcode;
  v_reclm_para.reriskcode:=rec_clm.reriskcode;
  v_reclm_para.DANGERUNITNO:=rec_clm.DANGERUNITNO;
  v_reclm_para.Uwyear:=to_number(to_char(rec_clm.startdate,'yyyy'));
  v_reclm_para.Dangertype:=rec_clm.dangertype;
  v_reclm_para.DangerCode:=rec_clm.dangerCode;
  v_reclm_para.DamageDATE:=trunc(rec_clm.damagedate);
  v_reclm_para.currency:=rec_clm.currency;
  v_reclm_para.paidsum:=rec_clm.OSsum;
  --����������ڴ��ڱ���ֹ�ڣ�����ڱ���ֹ��
  if rec_clm.DamageDATE>rec_clm.enddate then
     v_reclm_para.DamageDATE:=rec_clm.enddate;
  end if;
  --�����������С�ڱ������ڣ�����ڱ�������
  if rec_clm.DamageDATE<rec_clm.startDate then
     v_reclm_para.DamageDATE:=rec_clm.startDate;
  end if;
  g_errcode:='B4302';
  g_errmsg:='�ٷֳ���δ�������Ҳ����ֳ�ռ��';
   get_osnfac_ply_share(v_reclm_para, v_nfac_share, v_xlsharerate);

   if v_nfac_share.count >= 1 then
      --�Ѿ����ⰸ�Ǳ����ٷֲַ�����
      g_errcode:='B4303';
      g_errmsg:='�ٷֳ���δ�������γ��ٷֲַ�����';
      for n_seq in 1..v_nfac_share.count loop
         insert into REINS_OS_RECLM_N_FAC(
             OSRECLAIMNO   ,
             LAYERNO     ,
             CLAIMNO  ,
             POLICYNO  ,
             DANGERUNITNO   ,
             REPORTDATE     ,
             CURRENCY   ,
             OSSUM    ,
             CHGOSSM   ,
             RENPREM,
             CHGRENPREM      ,
             GENBILLNO  )
          select
             p_osreclaimno,
             v_nfac_share(n_seq).LAYERNO,
             rec_clm.CLAIMNO,
             rec_clm.POLICYNO ,
             rec_clm.DANGERUNITNO   ,
             grbilldeal.get_bill_date(rec_clm.REPORTDATE),
             rec_clm.currency,
             v_nfac_share(n_seq).PAIDSUM  / v_nfac_share(n_seq).exchrate,
             v_nfac_share(n_seq).CHGPAIDSUM / v_nfac_share(n_seq).exchrate,
             v_nfac_share(n_seq).RENPREM / v_nfac_share(n_seq).exchrate,
             v_nfac_share(n_seq).CHGRENPREM / v_nfac_share(n_seq).exchrate,
             ''
           from dual;
      end loop;
     g_errcode:='B4304';
     g_errmsg:='�ٷֳ���δ�������γ��ٷ��ٱ��˱�';
      --�Ѿ����ⰸ�Ǳ����ٷ��ٱ��˱�
      for rec_reins_share in cur_reins_share loop
         insert into REINS_OS_RECLM_N_FAC_REINS (
            OSRECLAIMNO,
            LAYERNO,
            BROKERCODE,
            REINSCODE,
            PAYCODE,
            AGENTCODE,
            SHARERATE,
            OSSUM,
            CHGOSSUM,
            BILLNO,
            FLAG
            )
         select OSRECLAIMNO,
                LAYERNO,
                rec_reins_share.BROKERCODE,
                rec_reins_share.REINSCODE,
                rec_reins_share.PAYCODE,
                rec_reins_share.AGENTCODE,
                rec_reins_share.SHARERATE,
                OSSUM * rec_reins_share.SHARERATE /100,  --modify by liupeng �������*�ֳ�������Ӧ�ó�100
                CHGOSSM * rec_reins_share.SHARERATE /100,
                null,
                null
            from REINS_OS_RECLM_N_FAC
           where OSRECLAIMNO = p_osreclaimno
             and LAYERNO = rec_reins_share.Layerno;
      end loop;
      --�ֲ��ٱ��˵����
      for rec_ossum in cur_ossum loop
        select sum(ossum), sum(chgossm)
         into v_ossum, v_chgossum
         from REINS_OS_RECLM_N_FAC
         where OSRECLAIMNO = p_osreclaimno
           and LAYERNO = rec_ossum.layerno;

        if abs(v_ossum-rec_ossum.ossum)>=0.01 or abs(v_chgossum-rec_ossum.chgossum)>=0.01 then
           update REINS_OS_RECLM_N_FAC_REINS set ossum=ossum + v_ossum-rec_ossum.ossum,
                                         chgossum=chgossum + v_chgossum-rec_ossum.chgossum
           where OSRECLAIMNO =p_osreclaimno
             and LAYERNO = rec_ossum.layerno
             and rownum = 1;
        end if;
      end loop;
   end if;

   --�����ٷֳַ����� -->дREINS_OS_RECLAIM_SHARE�ֳ���
   open cur_tty(to_number(to_char(rec_clm.startdate,'yyyy')), '92');--�����ٷֺ�Լ��Ҫ�ġ�
    fetch cur_tty into  rec_tty;
      n_seq:=0;
      for rec_clmnfac in cur_clmnfac loop
      n_seq:=n_seq+1;
      v_share(n_Seq).ttyid := rec_tty.ttyid;
      v_share(n_Seq).sectno := '0';
      v_share(n_seq).PCInd := 'P';
      v_share(n_seq).SHARERATE := rec_clmnfac.ossum*100/rec_clm.OSSum;
      v_share(n_seq).RICURR := rec_clm.currency;
      v_share(n_seq).EXCHRATE := 1;
      v_share(n_seq).PAIDSUM := rec_clmnfac.chgossm;
      v_xlOSSum := v_xlOSSum + v_share(n_seq).PAIDSUM;
     end loop;
   close cur_tty;

/*   --ʣ�ಿ�� �ŵ��������� -->дREINS_OS_RECLAIM_SHARE�ֳ���
   v_LeftOSSum := rec_clm.OSSum * v_xlsharerate - v_xlOSSum;
   if abs(v_LeftOSSum)>=0.01 then
    open cur_tty(to_number(to_char(rec_clm.startdate,'yyyy')), '82');
     fetch cur_tty into  rec_tty;
      n_seq:=n_seq+1;
      v_share(n_Seq).ttyid := rec_tty.ttyid;
      v_share(n_Seq).sectno := '0';
      v_share(n_seq).PCInd := 'P';
      v_share(n_seq).SHARERATE := v_LeftOSSum*100/(rec_clm.OSSum * v_xlsharerate);
      v_share(n_seq).RICURR := rec_clm.currency;
      v_share(n_seq).EXCHRATE := 1;
      v_share(n_seq).PAIDSUM := v_LeftOSSum;
    close cur_tty;
   end if;
*/
  g_errcode:='B4305';
  g_errmsg:='�ٷֳ���δ�������γ�REINS_OS_RECLAIM_SHARE��';
  crt_OSclm_abs(p_osreclaimno, v_share);

  p_message_code :='0';
  p_message_desc:='����ɹ�';
  update REINS_OS_RECLAIM set status='1',caldate=sysdate,date_updated=sysdate
   where OSreclaimno=p_osreclaimno;
  commit;

  exception when others then
   if v_error_Code='01' then
      p_message_code:='0';
      p_message_desc:='�˷�δ���ѽ��м���';
   else
      p_message_code:='1';
      p_message_desc:='����ʧ��';
   end if;

   g_errmsg:=g_errmsg||'-'||substr(sqlerrm,1,100);
   rollback;
   update REINS_OS_RECLAIM set status='2',date_updated=sysdate
    where OSreclaimno=p_osreclaimno ;
   insert into REINS_OS_CLAIM_ERR_LOG(errtype,osreclaimno,errcode,errmsg,errtime)
                        values('B4',p_osreclaimno,g_errcode,g_errmsg,sysdate);
   commit;
end nfac_OSclm_cal;

/* ������Լ�������غ��� */
procedure prop_contract_cal(p_type          in varchar2,
                            p_reply_para    in out reply_para,
                            p_arr_re_abs    in out arr_re_abs) is
--��Լ�ֱ��ƻ�
cursor cur_ttyplan(p_planadj_flag varchar2)
 is
 select openInd,ttyid,PriorityNo,LimitValue,TtyType,StatClass
 from
   (select a.openInd,a.ttyid,a.PriorityNo,0 LimitValue,c.TtyType,c.StatClass
     from REINS_TTY_PLAN a,REINS_TREATY b ,REINS_TTY_TABLE c
     where trunc(p_reply_para.startdate) between a.startdate and a.enddate
       and a.ttyid=b.ttyid
       and b.ttycode=c.ttycode
       and b.ttystatus in ('2','3')
       --and b.startdate<=p_reply_para.restartdate
       and p_planadj_flag='N'
       --add by 2013-07-11 �Ƿ�ũ������
       --and b.channel_class=p_reply_para.channelClass
       --modified by huangxf 2013/08/23 ����ͨ�������
       --AND b.channel_class = DECODE(b.channel_class, '*', '*', p_reply_para.channelClass)  -- wwp
   union all
   select a.openInd,a.ttyid,a.PriorityNo,a.LimitValue,c.TtyType,c.StatClass
     from REINS_PROP_PLAN_ADJ a,REINS_TREATY b,REINS_TTY_TABLE c
    where certino=p_reply_para.certino
      and DangerUnitNo=p_reply_para.DangerUnitNo
      and ReRiskCode=p_reply_para.ReRiskCode
      and RiskCode=p_reply_para.RiskCode
      and a.ttyid=b.ttyid
      and b.ttycode=c.ttycode
      --add by 2013-07-11 �Ƿ�ũ������
      --and b.channel_class=p_reply_para.channelClass
      --modified by huangxf 2013/08/23 ����ͨ�������
      --AND b.channel_class = DECODE(b.channel_class, '*', '*', p_reply_para.channelClass) -- wwp
      --and b.startdate<=p_reply_para.restartdate
      and b.ttystatus in ('2','3')
      and p_planadj_flag='Y'
      and p_type='0'
    union all
   select a.openInd,a.ttyid,a.PriorityNo,a.LimitValue,c.TtyType,c.StatClass
     from REINS_REPLY_PLAN_ADJ a,REINS_TREATY b,REINS_TTY_TABLE c
    where repolicyno=p_reply_para.repolicyno
      and ReEndortimes=p_reply_para.ReEndortimes
      and RecTimes = p_reply_para.RecTimes
      and a.ttyid=b.ttyid
      and b.ttycode=c.ttycode
      --add by 2013-07-11 �Ƿ�ũ������
      --and b.channel_class=p_reply_para.channelClass
      --modified by huangxf 2013/08/23 ����ͨ�������
      --AND b.channel_class = DECODE(b.channel_class, '*', '*', p_reply_para.channelClass) -- wwp
      --and b.startdate<=p_reply_para.restartdate
      and b.ttystatus in ('2','3')
      and p_type='1'
      and p_planadj_flag='Y'
     ) aa
     order by PriorityNo;
  rec_ttyplan cur_ttyplan%rowtype;
  /*cursor cur_propUnit is
     select * from REINS_PROP_UNIT where proposalno=p_reply_para.certiNo and dangerunitno=p_reply_para.DangerUnitNo and reriskcode = p_reply_para.reriskcode and riskcode=p_reply_para.riskcode;
  rec_propunit cur_propUnit%rowtype;
  cursor cur_repolicy is
     select * from REINS_REPOLICY where repolicyno=p_reply_para.repolicyno and rectimes=p_reply_para.RecTimes;
  rec_repolicy cur_repolicy%rowtype;
  cursor cur_tty(p_ttyid varchar2) is select b.StatClass,b.TtyType,a.ttycode,a.Uwyear
                 from REINS_TREATY a,REINS_TTY_TABLE b
                where a.ttyid=p_ttyid and a.ttycode=b.ttycode;
  rec_tty cur_tty%rowtype;*/

  n_cnt1 number(5):=0;
  v_planadj_flag varchar2(1):='N'; --�Ƿ���ڷֱ��ƻ�����
  --v_ttyid varchar2(9);
  --v_facshareRate number(3):=0;
  v_char char(1);
   c_count number(5):=0;
   c_count1 number(5):=0;
begin

   if p_type='0' then
     select count(*) into n_cnt1 from REINS_PROP_PLAN_ADJ
      where certino=p_reply_para.certino
        and DangerUnitNo=p_reply_para.DangerUnitNo
        and ReRiskCode=p_reply_para.ReRiskCode
        and RiskCode=p_reply_para.RiskCode;
   elsif p_type='1' then
     select count(*) into n_cnt1 from REINS_REPLY_PLAN_ADJ
      where RePolicyNo=p_reply_para.RePolicyNo
        and reendortimes=p_reply_para.reendortimes;
   end if;

   if n_cnt1>0 then
     v_planadj_flag:='Y';
   end if;
   open cur_ttyplan(v_planadj_flag);
   if p_type='1' then --���㲻����

     if p_reply_para.OriPML=0 then
       p_reply_para.OriPML := 1;
       p_reply_para.CurPML := 1;
     end if;
     /*if p_reply_para.OriPML=0 then
        g_errcode:='B1001';
        g_errmsg :='�ֱ�����PMLΪ0���ܼ���';
        select '*' into v_char from dual where 1=2;
     end if;*/

     fetch cur_ttyplan into rec_ttyplan;
     if cur_ttyplan%notfound then
          g_errmsg := 'û�зֱ��ƻ�';
          close cur_ttyplan;
          select '*' into v_char from dual where 1=2;
     end if;
   end if;

   close cur_ttyplan;
   open cur_ttyplan(v_planadj_flag);

   loop
     fetch cur_ttyplan into rec_ttyplan;
       exit when cur_ttyplan%notfound;
        case
           --����,��ͨ����
           --ԤԼ�ֱ�
             when rec_ttyplan.ttytype in ('11','12','13') and p_reply_para.curpml>0 then
                   /*if rec_ttyplan.ttyid = 'BUC0220140' or  rec_ttyplan.ttyid = 'BUC022015'  then

                    if p_type = '1' then
                    select count(*)
                      into c_count
                      from Reins_Repolicy a
                     where exists
                     (select *
                              from reins_repolicy g
                             where g.repolicyno = p_reply_para.repolicyno
                               and g.policyno = a.policyno)
                       and a.reriskcode in
                           (select reriskcode
                              from Reins_Code_Risk_To_Re_Risk b
                             where b.itemkind = '01'
                               and b.riskcode in ('0802', '0807','0808'))
                       and a.amount >= 300000
                       and a.amount <= 10000000;


                    if c_count > 0 then
                      select count(*)
                        into c_count1
                        from nbz_policy_item_motor c
                       where c.policyno in
                             (select a.policyno
                                from Reins_Repolicy a
                               where exists
                               (select *
                                        from reins_repolicy g
                                       where g.repolicyno =
                                             p_reply_para.repolicyno
                                         and g.policyno = a.policyno)
                                 and a.reriskcode in
                                     (select reriskcode
                                        from Reins_Code_Risk_To_Re_Risk b
                                       where b.itemkind = '01'
                                         and b.riskcode in ('0802', '0807','0808')))
                         and c.carusetype = '01'
                         and c.carkindcode = '100';
                    end if;
                    \*else
                      *\
                    end if;


                    if c_count1 > 0 then
                    if rec_ttyplan.openInd='N' or
                        (rec_ttyplan.openInd='Y' and
                         rec_ttyplan.limitvalue>0) then
                    share_cal(rec_ttyplan.ttyid,
                             rec_ttyplan.openInd,
                             rec_ttyplan.limitvalue,
                             p_reply_para,
                             p_arr_re_abs);
                    end if;
                    end if;


               else*/


               if rec_ttyplan.openInd='N' or
                  (rec_ttyplan.openInd='Y' and
                    rec_ttyplan.limitvalue>0) then
                   share_cal(rec_ttyplan.ttyid,
                             rec_ttyplan.openInd,
                             rec_ttyplan.limitvalue,
                             p_reply_para,
                             p_arr_re_abs);
               end if;

         /*end if;*/
           --��������,�����ǰ�ֱ����0 ��������Ϊ��
               --when rec_ttyplan.ttytype='81' and p_reply_para.curpml>0 then
             when rec_ttyplan.ttytype='81' then
               if p_reply_para.curpml>0 then
                 --�����ͬ��ͬ�¹�ͨ��ȡ�����Ʋ��ա������ա������գ���Σ�յ�λ�ۻ��ļ����߼�
                 /*if p_reply_para.Dangertype in ('A','B','C') then
                   p_reply_para.RetentValue:=get_c_retent(p_reply_para);
                  end if;*/
                  retent_cal(p_type,rec_ttyplan.ttyid,p_reply_para,p_arr_re_abs);
                else
                   p_reply_para.RetentValue:=0;
                end if;
           --������������
             when rec_ttyplan.ttytype ='21' and p_reply_para.RetentValue>0 then
               if rec_ttyplan.openInd='N' or
                  (rec_ttyplan.openInd='Y' and
                    rec_ttyplan.limitvalue>0) then
                   share_cal(rec_ttyplan.ttyid,
                             rec_ttyplan.openInd,
                             rec_ttyplan.limitvalue,
                             p_reply_para,
                             p_arr_re_abs);
               end if;
            --������
             when rec_ttyplan.ttytype='31' and p_reply_para.curpml>0 then
               if rec_ttyplan.openInd='N' or
                  (rec_ttyplan.openInd='Y' and
                    rec_ttyplan.limitvalue>0) then
                --   if p_reply_para.RetentValue<>0 then
                   surplus_cal(rec_ttyplan.ttyid,
                               rec_ttyplan.openInd,
                               rec_ttyplan.limitvalue,
                               p_reply_para,
                               p_arr_re_abs) ;
                --   end if;
               end if;
            else null;
        end case;
   end loop;
   close cur_ttyplan;

   --����Ƿ��ۼƣ����ұ������0,ȫ�������״�
   if p_reply_para.Dangertype not in ('A','B','C')
     and p_reply_para.curpml=p_reply_para.oripml
     and p_reply_para.curpml>0 and p_type='1' then
         g_errmsg := 'ȫ�������˸�������';
         select '*' into v_char from dual where 1=2;
   end if;

   g_errcode:='B1001';
   g_errmsg :='���������������';
   --������������
   if (p_reply_para.curpml>0 or p_reply_para.oripml=0 )   then
     affi_retent_cal(get_affi_ttyid(p_reply_para.uwyear),p_reply_para,p_arr_re_abs);
   end if;

   /*n_cnt1:=1;
   for n_cnt1 in 1..p_arr_re_abs.count loop
     p_arr_re_abs(n_cnt1).ttyid :=p_arr_re_abs(n_cnt1).ttyid;
     p_arr_re_abs(n_cnt1).shareRate :=p_arr_re_abs(n_cnt1).shareRate;
   end loop;*/

   g_errcode:='B1002';
   g_errmsg :='��������quota_reten_adj����';
   quota_reten_adj(p_arr_re_abs,p_reply_para);

  --����Ϊ�㱣�Ѳ�Ϊ�������½����ѷŵ���������
  /*if p_arr_re_abs.count=0 then
      if p_type='0' then
         select sum(shareRate) into v_facshareRate from REINS_ENQ_REINS where certino=p_reply_para.certiNo and dangerunitno=p_reply_para.DangerUnitNo and reriskcode = p_reply_para.reriskcode and riskcode=p_reply_para.riskcode and enquirytype='T';
         open cur_propUnit;
           fetch cur_propUnit into rec_propUnit;
            if cur_propUnit%found and rec_propUnit.grsprem>0  and v_facShareRate<100 then
                 v_ttyid:=get_affi_ttyid(p_reply_para.uwyear);
                 p_arr_re_abs(1).risum:=0;
                 p_arr_re_abs(1).RICurr:=p_reply_para.OriCurr;
                 p_arr_re_abs(1).ExchRate:=1;
                 p_arr_re_abs(1).ttyid:=v_ttyid;
                 open cur_tty(v_ttyid);
                    fetch cur_tty into rec_tty;
                 close cur_tty;
                 p_arr_re_abs(1).ttycode:=rec_tty.ttycode;
                 p_arr_re_abs(1).Uwyear:=rec_tty.Uwyear;
                 p_arr_re_abs(1).sectno:='0';
                 p_arr_re_abs(1).ttytype:=rec_tty.ttytype;
                 p_arr_re_abs(1).statclass:=rec_tty.statclass;
                 p_arr_re_abs(1).ShareRate:=100;
                 p_arr_re_abs(1).Netind:='0';
                p_arr_re_abs(1).PCind:='P';
            end if;
            close cur_propUnit;
      elsif p_reply_para.reendortimes='000' then
                   select sum(shareRate) into v_facshareRate from REINS_REPLY_FAC where repolicyno=p_reply_para.repolicyno and rectimes=p_reply_para.RecTimes;
          open cur_repolicy;
           fetch cur_repolicy into rec_repolicy;
            if cur_repolicy%found and rec_repolicy.grsprem>0  and v_facshareRate <100 then
                 v_ttyid:=get_affi_ttyid(p_reply_para.uwyear);
                 p_arr_re_abs(1).risum:=0;
                 p_arr_re_abs(1).RICurr:=p_reply_para.OriCurr;
                 p_arr_re_abs(1).ExchRate:=1;
                 p_arr_re_abs(1).ttyid:=v_ttyid;
                 open cur_tty(v_ttyid);
                    fetch cur_tty into rec_tty;
                 close cur_tty;
                 p_arr_re_abs(1).ttycode:=rec_tty.ttycode;
                 p_arr_re_abs(1).Uwyear:=rec_tty.Uwyear;
                 p_arr_re_abs(1).sectno:='0';
                 p_arr_re_abs(1).ttytype:=rec_tty.ttytype;
                 p_arr_re_abs(1).statclass:=rec_tty.statclass;
                 p_arr_re_abs(1).ShareRate:=100;
                 p_arr_re_abs(1).Netind:='0';
                p_arr_re_abs(1).PCind:='P';
            end if;
            close cur_repolicy;

     end if;
   end if;*/
end prop_contract_cal;

PROCEDURE quota_reten_adj( p_Arr_Re_Abs in out Arr_Re_Abs,
                           p_reply_para in reply_para )
is
/*  cursor cur_risk is
     select * from REINS_CODE_RE_RISK where reriskcode=trim(to_char(p_reply_para.reriskcode));
      rec_risk   cur_risk%rowtype;
     */
  rec_risk   REINS_CODE_RE_RISK%rowtype;

  cur_risk sys_refcursor;
  cursor cur_tty(p_ttyid REINS_REPLY_SHARE.ttyid%type) is
     select b.StatClass,b.TtyType,a.ttycode,a.Uwyear
       from REINS_TREATY a,REINS_TTY_TABLE b
      where a.ttyid=p_ttyid and a.ttycode=b.ttycode;
  rec_tty   cur_tty%rowtype;

  n_cnt number(3):=1;
  v_risum         REINS_REPLY_SHARE.Risum%type:=0;
  v_affi_ttyid    REINS_REPLY_SHARE.ttyid%type;
  v_findaffi      char(1):='0';
  v_char          char(1); --�����쳣��ֹ
  v_riskcode      varchar2(4);
begin
   v_riskcode:=p_reply_para.reriskcode;
  open cur_risk for select * from REINS_CODE_RE_RISK where reriskcode=v_riskcode;
  fetch cur_risk into rec_risk;
   if cur_risk%notfound then
     select '*' into v_char from dual where 1=2;
   end if;
  close cur_risk;

  --�ж��Ƿ�ͬ�����ֱ��ĸ�����
  if rec_risk.nacccalind='1' and rec_risk.respecialind='3' then
    --�����޶�Ϸ����ж�
    if p_reply_para.RetentValuelimit is null and p_reply_para.RetentValuelimit <= 0 then
      select '*' into v_char from dual where 1=2;
    end if;

    --����ë����
    for n_cnt in 1..p_Arr_Re_Abs.count loop
       if p_Arr_Re_Abs(n_cnt).TtyType in('81','21') then
          v_risum := v_risum+p_Arr_Re_Abs(n_cnt).risum;
       end if;
    end loop;

    --�ж�ë�����Ƿ������޶�
    if p_reply_para.RetentValuelimit < v_risum and v_risum != 0 then
      for n_cnt in 1..p_Arr_Re_Abs.count loop
         --�糬��������ë����=�����޶�
         if p_Arr_Re_Abs(n_cnt).TtyType in('81','21') then
            p_Arr_Re_Abs(n_cnt).risum := p_Arr_Re_Abs(n_cnt).risum * p_reply_para.RetentValuelimit/v_risum;
            p_Arr_Re_Abs(n_cnt).sharerate := p_Arr_Re_Abs(n_cnt).risum*100/p_reply_para.OriPML;
         end if;
      end loop;
      for n_cnt in 1..p_Arr_Re_Abs.count loop
         if p_Arr_Re_Abs(n_cnt).TtyType = '82' then
            --�������ַŵ���������
            if v_findaffi = '0' then
               p_Arr_Re_Abs(n_cnt).risum := p_Arr_Re_Abs(n_cnt).risum + (v_risum-p_reply_para.RetentValuelimit);
               p_Arr_Re_Abs(n_cnt).sharerate := p_Arr_Re_Abs(n_cnt).risum*100/p_reply_para.OriPML;
            end if;
            v_findaffi := '1';
         end if;
      end loop;

      --�總�����������ڣ���׷�Ӽ�¼
      if v_findaffi = '0' then
         v_affi_ttyid := get_affi_ttyid(p_reply_para.uwyear);
         open cur_tty(v_affi_ttyid);
         fetch cur_tty into rec_tty;
           if cur_tty%notfound then
             select '*' into v_char from dual where 1=2;
           end if;
         close cur_tty;

         n_cnt := p_Arr_Re_Abs.count+1;
         p_arr_re_abs(n_cnt).ttyid := v_affi_ttyid;
         p_arr_re_abs(n_cnt).risum:=v_risum-p_reply_para.RetentValuelimit;
         p_arr_re_abs(n_cnt).RICurr:=p_reply_para.OriCurr;
         p_arr_re_abs(n_cnt).ExchRate:=1;
         p_arr_re_abs(n_cnt).ttycode:=rec_tty.ttycode;
         p_arr_re_abs(n_cnt).Uwyear:=rec_tty.Uwyear;
         p_arr_re_abs(n_cnt).sectno:='0';
         p_arr_re_abs(n_cnt).ttytype:=rec_tty.ttytype;
         p_arr_re_abs(n_cnt).statclass:=rec_tty.statclass;
         if p_reply_para.OriPML != 0 then
            p_arr_re_abs(n_cnt).ShareRate:=100*p_arr_re_abs(n_cnt).RISum/p_reply_para.OriPML;
         else
            p_arr_re_abs(n_cnt).ShareRate:=0;
         end if;
         p_arr_re_abs(n_cnt).Netind:='0';
         p_arr_re_abs(n_cnt).PCind:='P';
      end if;
    end if;

  end if;
end quota_reten_adj;
/*������Լ���㣬���ڷֱ�/�������ֱ�����*/
procedure share_cal(p_ttyid         in REINS_TREATY.ttyid%type,
                    p_openInd       in REINS_TTY_PLAN.openInd%type,
                    p_fo_limit      in number,
                    p_reply_para    in out reply_para,
                    p_arr_re_abs    in out arr_re_abs ) is
cursor cur_tty is
     select a.MainCurr,b.StatClass,b.TtyType,a.ttycode,a.Uwyear
       from REINS_TREATY a,REINS_TTY_TABLE b
      where a.ttyid=p_ttyid and a.ttycode=b.ttycode;
rec_tty cur_tty%rowtype;

--ȡ���Ϸֱ�������section��ȡ�����Ʊ����ͷֳ��޶�
cursor cur_section is
     select b.sectno,b.Currency,b.RetentValue,b.ReinsRate,b.Lines,b.LimitValue,b.NetInd,
            --c.mCoilimitRate,c.InLimitRate,
            c.MainCurr,c.PCInd,c.STARTDATE,nvl(c.risharerate,100) risharerate
       from REINS_TTY_SECT_RISK a,REINS_TTY_SECT b,REINS_TREATY c
      where p_reply_para.comcode like a.comcode||'%'
        and a.reriskcode=p_reply_para.reriskcode
        and (a.channelcode=p_reply_para.channelcode or a.channelcode='9999')
        and b.ttyid=p_ttyid
        and a.ttyid=b.ttyid
        and a.sectno=b.sectno
        and b.ttyid=c.ttyid
        --add by 2013-07-10��Լ��Ҫ�����Ƿ�ũ����1��0��
        --and c.channel_class=p_reply_para.channelClass
        --add by 2013-07-10��Լ������Ҫ������ҵ���
        --and b.industry_class=p_reply_para.industryClass;
        --modified by huangxf 2013/08/10
        --AND c.channel_class = DECODE(c.channel_class, '*', '*', p_reply_para.channelClass)
        --AND b.industry_class = DECODE(b.industry_class, '*', '*', p_reply_para.industryClass)
        ;
rec_section cur_section%rowtype;
n_discount REINS_TTY_SECT.InLimitRateLower%type:=100;
n_cnt_arr number(4):=0;  --��Ž���������
n_tmp1 number(2):=0;
v_limitExch REINS_REPLY_SHARE.ExchRate%type;
v_ttyid   REINS_TREATY.ttyid%type;
v_pml_share REINS_REPOLICY.pml%type:=0;
v_quota_limit REINS_REPOLICY.pml%type:=0;
v_yylimit REINS_REPOLICY.pml%type:=0;
begin

   open cur_tty;
      fetch cur_tty into rec_tty;
   close cur_tty;
   open cur_section;
    fetch cur_section into rec_section;
      if cur_section%notfound then
          close cur_Section;
          return;
      end if;
   close cur_Section;

    --ȡ����/����ҵ��ֳ����Ʊ���
    n_discount := get_ttyid_discount('2', p_ttyid, p_reply_para);
    --dbms_output.put_line('=====L1==n_discount======='||n_discount);
    if n_discount <= 0 then
       return; --���ۿ�Ϊ0����ֱ�ӷ���
    end if;
/*
--ȡ����ҵ��ֳ����Ʊ���
   if p_reply_para.CoinsInd<>'0' then
      n_discount:=rec_section.mCoiLimitRate;
   end if;
--ȡת��ҵ��ֳ����Ʊ���
   if p_reply_para.BusinessInd<>'0' then
     n_discount:=rec_section.InLimitRate;
   end if;
*/

   n_cnt_arr:=p_arr_re_abs.count+1;
   v_ttyid:=get_last_ttyid(p_ttyid);
   --�ٱ������ж�
   select count(*) into n_tmp1 from REINS_TTY_CURR where ttyid=v_ttyid and Currency=p_reply_para.OriCurr;
   if n_tmp1>0 then
    p_arr_re_abs(n_cnt_arr).RICurr:=p_reply_para.OriCurr;
   else
    p_arr_re_abs(n_cnt_arr).RICurr:=rec_section.mainCurr;
   end if;
   --p_arr_re_abs(n_cnt_arr).ExchRate:=get_exchrate(p_reply_para.OriCurr,p_arr_re_abs(n_cnt_arr).RICurr,sysdate);
   --v_limitExch:=get_exchrate(p_arr_re_abs(n_cnt_arr).RICurr,rec_section.Currency,sysdate);
   --��Լ��ȵ�һ����ʣ���Ϊ�����Լ�޶�Ļ���
   p_arr_re_abs(n_cnt_arr).ExchRate:=get_exchrate(p_reply_para.OriCurr,p_arr_re_abs(n_cnt_arr).RICurr,rec_section.STARTDATE);
   v_limitExch:=get_exchrate(p_arr_re_abs(n_cnt_arr).RICurr,rec_section.Currency,rec_section.STARTDATE);
   p_arr_re_abs(n_cnt_arr).ttyid:=v_ttyid;
   p_arr_re_abs(n_cnt_arr).ttycode:=rec_tty.ttycode;
   p_arr_re_abs(n_cnt_arr).Uwyear:=rec_tty.Uwyear;
   p_arr_re_abs(n_cnt_arr).sectno:=rec_section.sectno;
   p_arr_re_abs(n_cnt_arr).ttytype:=rec_tty.ttytype;
   p_arr_re_abs(n_cnt_arr).statclass:=rec_tty.statclass;
   p_arr_re_abs(n_cnt_arr).NetInd:=rec_section.NetInd;
   if p_reply_para.dangertype not in ('A','B','C') then
     p_arr_re_abs(n_cnt_arr).PCInd:='P';
   else
     p_arr_re_abs(n_cnt_arr).PCInd:=rec_section.PCIND;
   end if;

--����������Լ����������İٷֱȿ۳���������������ǰ�������������Լ�ļ���
   if rec_tty.ttytype='21' then
      v_pml_share:=p_reply_para.retentvalue*rec_section.ReinsRate/100 * rec_section.risharerate/100;
   else
      v_pml_share:= p_reply_para.oriPML*rec_section.ReinsRate/100 * rec_section.risharerate/100;
   end if;

--����ǿ��ں�Լ���ֳ��޶�ȡ ָ���޶�ͺ�Լ�����޶����Сֵ
--modify by liupeng ���ں�Լ�ķֳ�������ǵ����ֱ��ƻ�¼��ķֳ�����
   if p_openInd='Y' /*and v_pml_share>p_fo_limit*/ then
     v_pml_share:=p_fo_limit;
   end if;

   if p_arr_re_abs(n_cnt_arr).PCInd='P'  then
     v_yylimit:=0;
   else
     v_yylimit:=get_yylimit(p_reply_para,
                         p_arr_re_abs(n_cnt_arr).ttyid,
                         p_arr_re_abs(n_cnt_arr).RICurr);
   end if;
   if v_pml_share*p_arr_re_abs(n_cnt_arr).ExchRate<(rec_section.LimitValue * rec_section.risharerate/100 *n_discount/(v_limitExch*100)-v_yylimit) then
        v_quota_limit:=v_pml_share;
   else
     if rec_section.LimitValue * rec_section.risharerate/100 *n_discount/(v_limitExch*100)-v_yylimit>=0 then
        v_quota_limit:=(rec_section.LimitValue * rec_section.risharerate/100 *n_discount/(v_limitExch*100)-v_yylimit);
     else
       v_quota_limit:=0;
     end if;
   end if;

   --20110802:�ϲ�¼������������������Լ�޶�
   if rec_tty.ttytype='21' and p_reply_para.combineInd='1' then
     v_quota_limit:=v_pml_share*p_arr_re_abs(n_cnt_arr).ExchRate;
   end if;

   if rec_tty.ttytype<>'21' then
     if  v_quota_limit>=p_reply_para.CurPML then
         p_arr_re_abs(n_cnt_arr).risum:=p_reply_para.CurPML* p_arr_re_abs(n_cnt_arr).ExchRate;
     else
         p_arr_re_abs(n_cnt_arr).risum:=v_quota_limit*p_arr_re_abs(n_cnt_arr).ExchRate;
     end if;
   else --����������������Ϻ�Ҫ���ָ������ķֳ�������Ӧ�ؼ�ȥ
     p_arr_re_abs(n_cnt_arr).risum:=v_quota_limit*p_arr_re_abs(n_cnt_arr).ExchRate;
     for i in 1..p_arr_re_abs.count loop
       if p_arr_re_abs(i).ttytype='81' then
          p_arr_re_abs(i).risum:=p_arr_re_abs(i).risum-v_quota_limit;
          p_arr_re_abs(i).sharerate:=p_arr_re_abs(i).risum*100/p_reply_para.OriPML;
       end if;
     end loop;
   end if;

   p_arr_re_abs(n_cnt_arr).ShareRate:=100*(p_arr_re_abs(n_cnt_arr).RISum/p_arr_re_abs(n_cnt_arr).ExchRate)/p_reply_para.OriPML;
   p_arr_re_abs(n_cnt_arr).tax:=p_reply_para.tax * p_arr_re_abs(n_cnt_arr).ShareRate/100;
   p_arr_re_abs(n_cnt_arr).taxInd :=p_reply_para.taxInd;
   if rec_tty.ttytype<>'21' then
      p_reply_para.CurPML:=p_reply_para.CurPML-p_arr_re_abs(n_cnt_arr).RISum/p_arr_re_abs(n_cnt_arr).ExchRate;
   end if;
end  share_cal;

/*����Լ���㣬���ڷֱ�/�������ֱ�����*/
procedure surplus_cal(p_ttyid         in REINS_TREATY.ttyid%type,
                      p_openInd       in REINS_TTY_PLAN.openInd%type,
                      p_fo_limit      in number,
                      p_reply_para    in out reply_para,
                      p_arr_re_abs    in out arr_re_abs ) is
cursor cur_section is
     select b.sectno,b.Currency,b.RetentValue,b.ReinsRate,b.Lines,b.LimitValue,b.NetInd,
            --c.MCoiLimitRate,c.InLimitRate,
            c.MainCurr,c.PCInd,c.STARTDATE
       from REINS_TTY_SECT_RISK a,REINS_TTY_SECT b,REINS_TREATY c
      where p_reply_para.comcode like a.comcode||'%'
        and a.reriskcode=p_reply_para.reriskcode
        and (a.channelcode=p_reply_para.channelcode or a.channelcode='9999')
        and b.ttyid=p_ttyid
        and a.ttyid=b.ttyid
        and a.sectno=b.sectno
        and b.ttyid=c.ttyid
        --add by 2013-07-11��Լ��Ҫ�����Ƿ�ũ����Y��N��
        --and b.industry_class=p_reply_para.industryClass
        --add by 2013-07-11��Լ������Ҫ������ҵ���
        --and c.channel_class=p_reply_para.channelClass;
        --modified by huangxf 2013/08/10
        --AND c.channel_class = DECODE(c.channel_class, '*', '*', p_reply_para.channelClass)
        --AND b.industry_class = DECODE(b.industry_class, '*', '*', p_reply_para.industryClass)
        ;
rec_section cur_section%rowtype;

cursor cur_tty is select a.MainCurr,b.StatClass,b.TtyType,a.ttycode,a.Uwyear
                 from REINS_TREATY a,REINS_TTY_TABLE b
                where a.ttyid=p_ttyid and a.ttycode=b.ttycode;
rec_tty cur_tty%rowtype;

n_discount  REINS_TTY_SECT.InLimitRateLower%type:=100;
n_cnt_arr   number(4):=0;  --��Ž���������
n_tmp1      number(2):=0;
v_limitExch REINS_REPLY_SHARE.ExchRate%type;
v_ttyid     REINS_TREATY.ttyid%type;
v_pml_share   REINS_REPOLICY.pml%type;
v_quota_limit REINS_REPOLICY.pml%type;
v_c_retent    REINS_REPOLICY.pml%type;
v_curr_RiSum  REINS_REPOLICY.pml%type :=0;
begin
   --����SpecialInd ȡSection
   open cur_section;
    fetch cur_section into rec_section;
     if cur_section%notfound then
        close cur_section;
        return;
     end if;
   close cur_Section;

   open cur_tty;
     fetch cur_tty into rec_tty;
   close cur_tty;

   v_ttyid:=get_last_ttyid(p_ttyid);
   --ȡ����/����ҵ��ֳ����Ʊ���
   n_discount := get_ttyid_discount('0', v_ttyid, p_reply_para);
   if n_discount <= 0 then
      return; --���ۿ�Ϊ0����ֱ�ӷ���
   end if;
/*
   if p_reply_para.CoinsInd<>'0' then
     n_discount:=rec_section.MCoiLimitRate;
   end if;
   if p_reply_para.BusinessInd<>'0' then
     n_discount:=rec_section.InLimitRate;
   end if;
*/

   n_cnt_arr := p_arr_re_abs.count+1;
   --�ٱ������ж�
   select count(*) into n_tmp1 from REINS_TTY_CURR
    where ttyid=v_ttyid
     and Currency=p_reply_para.OriCurr;
   if n_tmp1>0 then
     p_arr_re_abs(n_cnt_arr).RICurr:=p_reply_para.OriCurr;
   else
     p_arr_re_abs(n_cnt_arr).RICurr:=rec_section.mainCurr;
   end if;
   --p_arr_re_abs(n_cnt_arr).ExchRate:=get_exchrate(p_reply_para.OriCurr,p_arr_re_abs(n_cnt_arr).RICurr,sysdate);
   --v_limitExch:=get_exchrate(p_arr_re_abs(n_cnt_arr).RICurr,rec_section.Currency,sysdate);
   --��Լ��ȵ�һ����ʣ���Ϊ�����Լ�޶�Ļ���
   p_arr_re_abs(n_cnt_arr).ExchRate:=get_exchrate(p_reply_para.OriCurr,p_arr_re_abs(n_cnt_arr).RICurr,rec_section.STARTDATE);
   v_limitExch:=get_exchrate(p_arr_re_abs(n_cnt_arr).RICurr,rec_section.Currency,rec_section.STARTDATE);

   p_arr_re_abs(n_cnt_arr).ttyid:=v_ttyid;
   p_arr_re_abs(n_cnt_arr).ttycode:=rec_tty.ttycode;
   p_arr_re_abs(n_cnt_arr).Uwyear:=rec_tty.Uwyear;
   p_arr_re_abs(n_cnt_arr).sectno:=rec_section.sectno;
   p_arr_re_abs(n_cnt_arr).ttytype:=rec_tty.ttytype;
   p_arr_re_abs(n_cnt_arr).statclass:=rec_tty.statclass;
   p_arr_re_abs(n_cnt_arr).NetInd:=rec_section.NetInd;
   if p_reply_para.dangertype not in ('A','B','C') then
     p_arr_re_abs(n_cnt_arr).PCInd:='P';
   else
     p_arr_re_abs(n_cnt_arr).PCInd:=rec_section.PCIND;
   end if;

   v_c_retent:=p_reply_para.RetentValue*n_discount/100;
   if p_reply_para.dangertype in ('A','B','C') then
      g_errcode:='B1009';
      g_errmsg :='��ȡ�����ۻ����޶�';
      v_c_retent:=get_danger_limit(p_reply_para);
   end if;
   v_pml_share:=v_c_retent*rec_section.lines;
   if p_reply_para.dangertype in ('A','B','C') then
      g_errcode:='B1010';
      g_errmsg :='��ȡ�����ۻ��ѷ��޶�';
      v_curr_RiSum:=get_danger_RiSum(p_reply_para,v_ttyid,p_reply_para.DangerType);
      if (v_pml_share-v_curr_RiSum)>=0 then
        v_pml_share:=(v_pml_share-v_curr_RiSum)*n_discount/100;
      else
         v_pml_share:=0;
      end if;
   else
     v_pml_share:=v_pml_share;
   end if;
/*�����ԤԼ��Լ����ֳ�����Ϊ�����������������Լ�޶���������Ʊ�����
�������Ʊ���������Сֵ*/
   if p_openInd='Y' /*and v_pml_share>p_fo_limit*/  then
    v_pml_share:=p_fo_limit;
   end if;

   if v_pml_share*v_limitExch*p_arr_re_abs(n_cnt_arr).ExchRate<rec_section.LimitValue*n_discount/100 then
     v_quota_limit:=v_pml_share*p_arr_re_abs(n_cnt_arr).ExchRate;
   else
     v_quota_limit:=(rec_section.LimitValue*n_discount/100)/v_limitExch;
   end if;
  --����Ǻϲ�¼��������Ϻ�Լ���޶�ֱ�Ӿ͵���(������*����)������Ҫ���Լ�����޶�Ƚ�
  if p_reply_para.combineInd='1' then
    v_quota_limit:=v_pml_share*p_arr_re_abs(n_cnt_arr).ExchRate;
  end if;

  if  v_quota_limit>=p_reply_para.CurPML*p_arr_re_abs(n_cnt_arr).ExchRate then
    p_arr_re_abs(n_cnt_arr).risum:=p_reply_para.CurPML* p_arr_re_abs(n_cnt_arr).ExchRate;
  else
   p_arr_re_abs(n_cnt_arr).risum:=v_quota_limit;
  end if;
  p_arr_re_abs(n_cnt_arr).ShareRate:=100*(p_arr_re_abs(n_cnt_arr).RISum/p_arr_re_abs(n_cnt_arr).ExchRate)/p_reply_para.OriPML;
  p_reply_para.CurPML:=p_reply_para.CurPML-p_arr_re_abs(n_cnt_arr).RISum/p_arr_re_abs(n_cnt_arr).ExchRate;
  --modify by wangmx 20170728
  p_arr_re_abs(n_cnt_arr).tax := p_reply_para.tax * p_arr_re_abs(n_cnt_arr).ShareRate/100;
  p_arr_re_abs(n_cnt_arr).taxInd := p_reply_para.taxInd;
end surplus_cal;

/*������Լ���㣬p_reply_para.RetentValue�ڵ�����������ǰ�����¼���
���Σ������Ϊ('A','B','C')�ģ���'����'��'����'��'������'���ģ�
��Σ�յ�λ��Ϣ���壨REINS_POLICY_UNIT����ȡָ���������������ӷֱ����
��REINS_REPLY_SHARE����ȡ����ǰΣ�յ�λ���ѷ�����������ȡ��ֵΪ���ηֱ�������������ԭʼ���ʣ�ౣ������ȡ��Сֵ��Ϊ���ηֱ���������
*/
procedure retent_cal(p_type          in varchar2,
                     p_ttyid         in REINS_TREATY.ttyid%type,
                     p_reply_para    in out reply_para,
                     p_arr_re_abs    in out arr_re_abs ) is
cursor cur_tty is select b.StatClass,b.TtyType,a.ttycode,a.Uwyear
                 from REINS_TREATY a,REINS_TTY_TABLE b
                where a.ttyid=p_ttyid and a.ttycode=b.ttycode;
  rec_tty cur_tty%rowtype;
  n_cnt_arr number(4):=0;  --��Ž���������
  n_surplus_ttyid  REINS_TREATY.ttyid%type;
begin
  open cur_tty;
   fetch cur_tty into rec_tty;
  close cur_tty;

  if p_reply_para.RetentValue<>0 then
     n_cnt_arr:=p_arr_re_abs.count+1;
     p_arr_re_abs(n_cnt_arr).RICurr:=p_reply_para.OriCurr;
     p_arr_re_abs(n_cnt_arr).ExchRate:=1;
     p_arr_re_abs(n_cnt_arr).ttyid:=p_ttyid;
     p_arr_re_abs(n_cnt_arr).ttycode:=rec_tty.ttycode;
     p_arr_re_abs(n_cnt_arr).Uwyear:=rec_tty.Uwyear;
     p_arr_re_abs(n_cnt_arr).sectno:='0';
     p_arr_re_abs(n_cnt_arr).ttytype:=rec_tty.ttytype;
     p_arr_re_abs(n_cnt_arr).statclass:=rec_tty.statclass;
     p_arr_re_abs(n_cnt_arr).Netind:='0';
     p_arr_re_abs(n_cnt_arr).PCind:='P';
     /*if p_reply_para.DangerType in ('A','B','C') then
        p_arr_re_abs(n_cnt_arr).PCind:='C';
     else
        p_arr_re_abs(n_cnt_arr).PCind:='P';
     end if;*/

     --liuxd add 2009.6.29 ��ǰ�ж��Ƿ������
     n_surplus_ttyid := Judge_Surplus(p_type, p_reply_para);
     if n_surplus_ttyid is not null then
        p_reply_para.RetentValue := p_reply_para.RetentValue * get_ttyid_discount('1', n_surplus_ttyid, p_reply_para)/100;
     end if;

     if p_reply_para.RetentValue>p_reply_para.CurPML then
        p_arr_re_abs(n_cnt_arr).RISum:=p_reply_para.CurPML;
     else
        p_arr_re_abs(n_cnt_arr).RISum:=p_reply_para.RetentValue;
     end if;
     p_arr_re_abs(n_cnt_arr).ShareRate:=100*p_arr_re_abs(n_cnt_arr).RISum/p_reply_para.OriPML;
     p_reply_para.CurPML:=p_reply_para.CurPML-p_arr_re_abs(n_cnt_arr).RISum;
     p_reply_para.retentvalue:=p_arr_re_abs(n_cnt_arr).RISum;
     --modify by wangmx 20170728
     p_arr_re_abs(n_cnt_arr).tax:= p_reply_para.tax * p_arr_re_abs(n_cnt_arr).ShareRate/100;
     p_arr_re_abs(n_cnt_arr).taxInd := p_reply_para.taxInd;
  end if;

end  retent_cal;
/*����������Լ���㣬p_reply_para.CurPML��Ϊ0ʱ���ô˷���*/
procedure affi_retent_cal(p_ttyid         in REINS_TREATY.ttyid%type,
                          p_reply_para    in out reply_para,
                          p_arr_re_abs    in out arr_re_abs ) is
cursor cur_tty is select b.StatClass,b.TtyType,a.ttycode,a.Uwyear
                 from REINS_TREATY a,REINS_TTY_TABLE b
                where a.ttyid=p_ttyid and a.ttycode=b.ttycode;
rec_tty cur_tty%rowtype;
n_cnt_arr number(4):=0;  --��Ž���������
begin
     open cur_tty;
      fetch cur_tty into rec_tty;
     close cur_tty;

       n_cnt_arr:=p_arr_re_abs.count+1;
       p_arr_re_abs(n_cnt_arr).risum:=p_reply_para.CurPML;
       p_arr_re_abs(n_cnt_arr).RICurr:=p_reply_para.OriCurr;
       p_arr_re_abs(n_cnt_arr).ExchRate:=1;
       p_arr_re_abs(n_cnt_arr).ttyid:=p_ttyid;
       p_arr_re_abs(n_cnt_arr).ttycode:=rec_tty.ttycode;
       p_arr_re_abs(n_cnt_arr).Uwyear:=rec_tty.Uwyear;
       p_arr_re_abs(n_cnt_arr).sectno:='0';
       p_arr_re_abs(n_cnt_arr).ttytype:=rec_tty.ttytype;
       p_arr_re_abs(n_cnt_arr).statclass:=rec_tty.statclass;
       if p_reply_para.OriPML!=0 then
          p_arr_re_abs(n_cnt_arr).ShareRate:=100*p_arr_re_abs(n_cnt_arr).RISum/p_reply_para.OriPML;
       else
          p_arr_re_abs(n_cnt_arr).ShareRate:=0;
        end if;
       p_reply_para.CurPML:=0;
       p_arr_re_abs(n_cnt_arr).Netind:='0';
       p_arr_re_abs(n_cnt_arr).PCind:='P';
       --modify by wangmx 20170728
       p_arr_re_abs(n_cnt_arr).Tax := p_reply_para.tax * p_arr_re_abs(n_cnt_arr).ShareRate/100;
       p_arr_re_abs(n_cnt_arr).TaxInd := p_reply_para.taxInd;
end affi_retent_cal;

/*����REINS_REPLY_SHARE���������ṹ���Ƶ�p_arr_re_abs����ֵ����REINS_REPLY_SHARE��
�������Ļ��������ȵ������������һ���ֱ�������*/
procedure crt_ply_abs(p_repolicyno in REINS_REPOLICY.repolicyno%type,
                      p_RecTimes   in REINS_REPOLICY.RecTimes%type,
                      p_arr_re_abs in  arr_re_abs  ) is
cursor cur_ply is
  select * from REINS_REPOLICY
   where repolicyno=p_repolicyno and RecTimes=p_Rectimes;
rec_ply cur_ply%rowtype;
n_Seq number(4);
v_RISum REINS_REPOLICY.pml%type:=0;
v_GrsPrem REINS_REPOLICY.pml%type:=0;
v_NetPrem REINS_REPOLICY.pml%type:=0;
v_findretnind varchar2(1):='N';
--�ٷֱַ���
--�ٷֱַ�����������
v_facRepolicyNo REINS_REPOLICY.repolicyno%type;
v_facRecTimes  REINS_REPOLICY.rectimes%type;
v_facGrsPrem REINS_REPOLICY.pml%type:=0;
v_facNetPrem REINS_REPOLICY.pml%type:=0;
v_facAmount REINS_REPOLICY.pml%type:=0;
v_facStatus REINS_REPOLICY.status%type;
v_facMessageCode varchar2(20);
v_facMessage varchar2(60);
v_char varchar2(1);
v_nfac_sharerate REINS_REPOLICY.sharerate%type:=0;

begin
 open cur_ply;
  fetch cur_ply into rec_ply;
 close cur_ply;

 for n_Seq in 1..p_arr_re_abs.count loop
   v_RISum:=v_RISum+p_arr_re_abs(n_seq).RISum/p_arr_re_abs(n_seq).exchrate;
   v_GrsPrem:=v_GrsPrem+p_arr_re_abs(n_seq).GrsPrem/p_arr_re_abs(n_seq).exchrate;
   v_NetPrem:=v_NetPrem+p_arr_re_abs(n_seq).NetPrem/p_arr_re_abs(n_seq).exchrate;
   if p_arr_re_abs(n_seq).ttytype='81'  then
     v_findretnind:='Y';
   end if;

   --IF p_arr_re_abs(N_SEQ).RISUM<>0 THEN
   g_errmsg := '���ɷֳ���ʧ��'||p_arr_re_abs(n_seq).ttyid;
   IF p_arr_re_abs(N_SEQ).sharerate<>0 or
      p_arr_re_abs(N_SEQ).RISum<>0 or
      p_arr_re_abs(N_SEQ).RIPrem<>0 THEN
    insert into REINS_REPLY_SHARE(
        RepolicyNo     ,
        ReferNo        ,
        TtyID          ,
        TtyCode        ,
        UwYear         ,
        RecTimes       ,
        PolicyNo       ,
        DangerUnitNo   ,
        DANGERTYPE  ,
        DangerCode     ,
        ComCode        ,
        RiskCode       ,
        businessind,
        ReRiskCode     ,
        PlyYear        ,
        StatClass      ,
        TtyType        ,
        SectNo         ,
        startdate      ,
        EndDate        ,
        Restartdate    ,
        ReEndDate      ,
        ShareRate      ,
        OriCurr        ,
        RICurr         ,
        ExchRate       ,
        RISum          ,
        GrsPrem        ,
        NetPrem        ,
        RIPrem        ,
        Ricomm        ,
        PCIND         ,
        NETIND        ,
        ACCCHANNEL,
        PlanCode,
        date_created,
        date_updated,
        tax,--modify by wangmx 20170728
        taxInd
        )
       select
        rec_ply.RepolicyNo     ,
        null                          ,
        p_arr_re_abs(n_seq).TtyID          ,
        p_arr_re_abs(n_seq).TtyCode        ,
        p_arr_re_abs(n_seq).UwYear         ,
        rec_ply.RecTimes       ,
        rec_ply.PolicyNo       ,
        rec_ply.DangerUnitNo    ,
        rec_ply.DANGERTYPE     ,
        rec_ply.DangerCode     ,
        rec_ply.ComCode        ,
        rec_ply.RiskCode      ,
        rec_ply.businessind,
        rec_ply.ReRiskCode     ,
        to_number(to_char(rec_ply.startdate,'yyyy'))        ,
        p_arr_re_abs(n_seq).StatClass      ,
        p_arr_re_abs(n_seq).TtyType        ,
        p_arr_re_abs(n_seq).SectNo         ,
        rec_ply.startdate      ,
        rec_ply.EndDate        ,
        rec_ply.Restartdate    ,
        rec_ply.ReEndDate      ,
        p_arr_re_abs(n_seq).ShareRate      ,
        rec_ply.Currency        ,
        p_arr_re_abs(n_seq).RICurr         ,
        p_arr_re_abs(n_seq).ExchRate       ,
        p_arr_re_abs(n_seq).RISum ,
        p_arr_re_abs(n_seq).GrsPrem ,
        p_arr_re_abs(n_seq).NetPrem ,
        p_arr_re_abs(n_seq).RIPrem ,
        nvl(p_arr_re_abs(n_seq).ricomm, 0),
        p_arr_re_abs(n_seq).PCInd,
        p_arr_re_abs(n_seq).NETIND,
        rec_ply.ACCCHANNEL,
        rec_ply.PlanCode,
        sysdate,
        sysdate,
        p_arr_re_abs(n_seq).tax,--modify by wangmx 20170728
        rec_ply.taxind
      from dual;
   END IF;
 end loop;

  --�ҳ��ٷֳ������v_nfac_sharerate
  begin
   select a.sharerate
     into v_nfac_sharerate
     from REINS_REPOLICY a,REINS_REPOLICY b
     where a.policyno=b.policyno
       and a.dangerunitno=b.dangerunitno
       and a.riskcode=b.riskcode
       and a.reriskcode=b.reriskcode
       and a.reinstype='2'
       and b.repolicyno=p_repolicyno
       and b.rectimes=p_rectimes
       and a.status in('0','1','2');
  exception when others then
   v_nfac_sharerate:=0;
  end;

  --�����ٷֲ��ֱ������
  --����ֻ��������Լ�ͱ����ٷ֣�����ʱ����Ҫ��ȥ�ٷֳ������
  --�ٷֳ������ʱ�ᵥ������
 if rec_ply.reinstype='0' and rec_ply.shareRate<100-v_nfac_sharerate then
   g_errmsg := '��������ٷֱַ�������';
   select a.repolicyno,a.rectimes,a.status
     into v_facRepolicyNo,v_facRecTimes,v_facStatus
     from REINS_REPOLICY a,REINS_REPOLICY b
     where a.policyno=b.policyno
         and a.dangerunitno=b.dangerunitno
         and a.riskcode=b.riskcode
         and a.reriskcode=b.reriskcode
         and a.reinstype='1'
         and b.repolicyno=p_repolicyno
         and a.status in('0','1','2')
         and b.rectimes=p_rectimes;
     if v_facStatus!='1' then
        fac_ply_cal(v_facRepolicyNo,v_facRecTimes,v_facMessageCode,v_facMessage);
        if v_facMessageCode!='0' then
          select '*' into v_char from dual where 1=2;
        end if;
     end if;
     --�۳�ԭ��
     select sum(risum/exchrate),sum(grsprem/exchrate),sum(netPrem/exchrate)
      into v_facAmount,v_facGrsPrem,v_facNetPrem
      from REINS_REPLY_FAC
      where repolicyno=v_facRepolicyNo and rectimes=v_facRecTimes;
     --������Լ�ͱ����ٷּ������ı��ë���Ѻ;�����
     v_RISum:=v_RISum+v_facAmount;
     v_GrsPrem:=v_GrsPrem+v_facGrsPrem;
     v_NetPrem:=v_NetPrem+v_facNetPrem;
 end if;

 --���ȵ���������о������ȵ�������������Լ�Ž��е���
 if rec_ply.sharerate=100-v_nfac_sharerate or v_facAmount<>0 then
  if abs(rec_ply.pml*(100-v_nfac_sharerate)/100-v_RISum)>=0.01 or
     abs(rec_ply.grsprem*(100-v_nfac_sharerate)/100-v_GrsPrem)>=0.01 or
     abs(rec_ply.NetPrem*(100-v_nfac_sharerate)/100-v_NetPrem)>=0.01 then
    --��Լ����
    if  rec_ply.reinstype='0' then
      if v_findretnind='Y' then
          update  REINS_REPLY_SHARE set RISum =RISum+(rec_ply.pml*(100-v_nfac_sharerate)/100-v_RISum),
                                grsprem=grsprem+(rec_ply.grsprem*(100-v_nfac_sharerate)/100-v_GrsPrem),
                                NetPrem=NetPrem+(rec_ply.NetPrem*(100-v_nfac_sharerate)/100-v_NetPrem),
                                Riprem=decode(netind,0,grsprem,netprem),date_updated=sysdate
          where  repolicyno=p_repolicyno and RecTimes=p_Rectimes  and ttytype='81';
       else
          update REINS_REPLY_SHARE set RISum =RISum+(rec_ply.pml*(100-v_nfac_sharerate)/100-v_RISum)*exchrate,
                                grsprem=grsprem+(rec_ply.grsprem*(100-v_nfac_sharerate)/100-v_GrsPrem)*exchrate,
                                NetPrem=NetPrem+(rec_ply.NetPrem*(100-v_nfac_sharerate)/100-v_NetPrem)*exchrate,
                                Riprem=decode(netind,0,grsprem,netprem),date_updated=sysdate
          where repolicyno=p_repolicyno and RecTimes=p_Rectimes and risum=(select max(risum) from REINS_REPLY_SHARE b where b.repolicyno=p_repolicyno and b.rectimes=p_RecTimes) and rownum=1;
      end if;
    end if;
    update REINS_REPLY_SHARE set RIprem =decode(netInd,'0',Grsprem,Netprem),date_updated=sysdate
      where repolicyno=p_repolicyno and RecTimes=p_Rectimes ;
  end if;
 end if;

end  crt_ply_abs;

/*����REINS_REENDR_SHARE���������ṹ���Ƶ�p_arr_re_abs����ֵ����REINS_REENDR_SHARE��
�������Ļ��������ȵ������������һ���ֱ�������*/
procedure crt_edr_abs(p_repolicyno in REINS_REPOLICY.repolicyno%type,
                      p_reendortimes in REINS_REPOLICY.reendortimes%type,
                      p_RecTimes   in REINS_REPOLICY.RecTimes%type,
                      p_arr_re_abs in  arr_re_abs  ) is
cursor cur_edr is
   select * from REINS_REENDOR
    where repolicyno=p_repolicyno
      and reendortimes=p_reendortimes
      and RecTimes=p_Rectimes;
rec_edr cur_edr%rowtype;
n_Seq number(4);
v_RISum REINS_REPOLICY.pml%type:=0;
v_GrsPrem REINS_REPOLICY.pml%type:=0;
v_NetPrem REINS_REPOLICY.pml%type:=0;
v_chgRISum REINS_REPOLICY.pml%type:=0;
v_chgGrsPrem REINS_REPOLICY.pml%type:=0;
v_chgNetPrem REINS_REPOLICY.pml%type:=0;
v_findretnind varchar2(1):='N';
--�ٷֱַ���
--�ٷֱַ�����������
v_facRepolicyNo REINS_REPOLICY.repolicyno%type;
v_facReendrTimes REINS_REENDOR.reendortimes%type;
v_facRecTimes  REINS_REPOLICY.rectimes%type;
v_facGrsPrem REINS_REPOLICY.pml%type:=0;
v_facNetPrem REINS_REPOLICY.pml%type:=0;
v_facAmount REINS_REPOLICY.pml%type:=0;
v_facChgRISum REINS_REENDR_FAC.Chgrisum%type:=0;
v_facChgGrsPrem REINS_REENDR_FAC.Chggrsprem%type:=0;
v_facChgNetPrem REINS_REENDR_FAC.Chgnetprem%type:=0;
v_facStatus REINS_REPOLICY.status%type;
v_facMessageCode varchar2(20);
--v_facMessage varchar2(60);
v_char varchar2(1);
v_nfac_sharerate REINS_REPOLICY.sharerate%type:=0;
v_facMessage varchar2(50):=0;
begin
 open cur_edr;
  fetch cur_edr into rec_edr;
 close cur_edr;
 for n_Seq in 1..p_arr_re_abs.count loop
   v_RISum:=v_RISum+p_arr_re_abs(n_seq).RISum/p_arr_re_abs(n_seq).exchrate;
   v_GrsPrem:=v_GrsPrem+p_arr_re_abs(n_seq).GrsPrem/p_arr_re_abs(n_seq).exchrate;
   v_NetPrem:=v_NetPrem+p_arr_re_abs(n_seq).NetPrem/p_arr_re_abs(n_seq).exchrate;
   v_ChgRISum:=v_ChgRISum+p_arr_re_abs(n_seq).ChgRISum/p_arr_re_abs(n_seq).exchrate;
   v_ChgGrsPrem:=v_ChgGrsPrem+p_arr_re_abs(n_seq).ChgGrsPrem/p_arr_re_abs(n_seq).exchrate;
   v_ChgNetPrem:=v_ChgNetPrem+p_arr_re_abs(n_seq).ChgNetPrem/p_arr_re_abs(n_seq).exchrate;
    if p_arr_re_abs(n_seq).ttytype='81' then
      v_findretnind:='Y';
    end if;
    g_errmsg := '���ɷֳ���ʧ��'||p_arr_re_abs(n_seq).ttyid;
    insert into REINS_REENDR_SHARE
    (REPOLICYNO     ,
     REendortimes ,
     RECTIMES       ,
     TTYID          ,
     RISKCODE       ,
     REFERNO        ,
     TTYCODE        ,
     UWYEAR         ,
     PCIND          ,
     POLICYNO       ,
     DANGERUNITNO   ,
     DANGERCODE     ,
     DANGERTYPE     ,
     COMCODE        ,
     RERISKCODE     ,
     businessind,
     PLYYEAR        ,
     STATCLASS      ,
     TTYTYPE        ,
     SECTNO         ,
     startdate      ,
     ENDDATE        ,
     REstartdate    ,
     REENDDATE      ,
     SHARERATE      ,
     ORICURR        ,
     RICURR         ,
     EXCHRATE       ,
     RISUM          ,
     GEARNEDPREM    ,
     GPORTFPREM     ,
     NEARNEDPREM    ,
     NPORTFPREM     ,
     GRSPREM        ,
     NETPREM        ,
     RIPREM         ,
     RICOMM         ,
     CHGRISUM       ,
     CHGGRSPREM     ,
     CHGNETPREM     ,
     CHGRIPREM      ,
     CHGRICOMM      ,
     NETIND         ,
     ACCCHANNEL,
     PlanCode,
     date_created,
     date_updated,
     tax,--modify by wangmx 20170728
     taxInd,
     changetax,
     vatAddTax,
     chgVatAddTax)
    select
     rec_edr.REPOLICYNO    ,
     rec_edr.REendortimes ,
     rec_edr.RECTIMES       ,
     p_arr_re_abs(n_seq).TTYID ,
     rec_edr.RISKCODE ,
     null, --�ο���
     p_arr_re_abs(n_seq).TTYCODE,
     p_arr_re_abs(n_seq).UWYEAR,
     p_arr_re_abs(n_seq).PCIND ,
     rec_edr.POLICYNO,
     rec_edr.DANGERUNITNO,
     rec_edr.DANGERCODE ,
     rec_edr.DANGERTYPE  ,
     rec_edr.COMCODE     ,
     rec_edr.RERISKCODE  ,
     rec_edr.businessind,
     to_number(to_char(rec_edr.startdate,'yyyy')) ,
     p_arr_re_abs(n_seq).STATCLASS ,
     p_arr_re_abs(n_seq).TTYTYPE  ,
     p_arr_re_abs(n_seq).SECTNO   ,
     rec_edr.startdate ,
     rec_edr.ENDDATE   ,
     rec_edr.REstartdate ,
     rec_edr.REENDDATE ,
     p_arr_re_abs(n_seq).SHARERATE ,
     rec_edr.currency ,
     p_arr_re_abs(n_seq).RICURR ,
     p_arr_re_abs(n_seq).EXCHRATE    ,
     p_arr_re_abs(n_seq).RISUM       ,
     p_arr_re_abs(n_seq).GEARNEDPREM ,
     p_arr_re_abs(n_seq).GPORTFPREM  ,
     p_arr_re_abs(n_seq).NEARNEDPREM ,
     p_arr_re_abs(n_seq).NPORTFPREM  ,
     p_arr_re_abs(n_seq).GRSPREM     ,
     p_arr_re_abs(n_seq).NETPREM     ,
     p_arr_re_abs(n_seq).RIPREM      ,
     nvl(p_arr_re_abs(n_seq).ricomm,0)      ,
     p_arr_re_abs(n_seq).CHGRISUM    ,
     p_arr_re_abs(n_seq).CHGGRSPREM  ,
     p_arr_re_abs(n_seq).CHGNETPREM  ,
     p_arr_re_abs(n_seq).CHGRIPREM   ,
     p_arr_re_abs(n_seq).chgricomm   ,
     p_arr_re_abs(n_seq).NETIND      ,
     rec_edr.ACCCHANNEL,
     rec_edr.PlanCode,
     sysdate,
     sysdate,
     p_arr_re_abs(n_seq).tax,--modify by wangmx 20170728
     p_arr_re_abs(n_seq).taxInd,
     p_arr_re_abs(n_seq).changetax,
     p_arr_re_abs(n_seq).vataddtax,
     p_arr_re_abs(n_seq).chgvataddtax
    from dual;
  end loop;

  --�����ٷֳ������������ʱ��
  begin
    /*select a.sharerate
       into v_nfac_sharerate
       from REINS_REENDOR a,REINS_REENDOR b
       where a.policyno=b.policyno
         and a.dangerunitno=b.dangerunitno
         and a.riskcode=b.riskcode
         and a.reriskcode=b.reriskcode
         and a.endortimes=b.endortimes
         and a.reinstype='2'
         and b.repolicyno=p_repolicyno
         and a.status in('0','1','2')
         and b.reendortimes=p_reendortimes
         and b.rectimes=p_rectimes;*/

     select a.sharerate
     into v_nfac_sharerate
     from REINS_REPOLICY a,REINS_REPOLICY b
     where a.policyno=b.policyno
       and a.dangerunitno=b.dangerunitno
       and a.riskcode=b.riskcode
       and a.reriskcode=b.reriskcode
       and a.reinstype='2'
       and b.repolicyno=p_repolicyno
       --and b.rectimes=p_rectimes
       and a.status in('0','1','2');
   exception when others then
     v_nfac_sharerate:=0;
   end;
  --�����ٷֲ��ֱ������
  --����ֻ��������Լ�ͱ����ٷ֣�����ʱ����Ҫ��ȥ�ٷֳ������
  --�ٷֳ������ʱ�ᵥ������
  --modify by liupeng 20160628
  --���ڷǱ����ٷֵ���������⣬������ڷǱ����ٷֲ�����
  if v_nfac_sharerate=0 then
    if rec_edr.reinstype='0' and rec_edr.shareRate<100-v_nfac_sharerate then
       g_errmsg := '��������ٷַ���������';
       select a.repolicyno,a.rectimes,a.reendortimes,a.status
       into v_facRepolicyNo,v_facRecTimes,v_facReendrTimes,v_facStatus
       from REINS_REENDOR a,REINS_REENDOR b
       where a.policyno=b.policyno
           and a.dangerunitno=b.dangerunitno
           and a.riskcode=b.riskcode
           and a.reriskcode=b.reriskcode
           and a.endortimes=b.endortimes
           and a.reinstype='1'
           and b.repolicyno=p_repolicyno
           and a.status in('0','1','2')
           and b.reendortimes=p_reendortimes
           and b.rectimes=p_rectimes;
       if v_facStatus<>'1' then
          fac_edr_cal(v_facRepolicyNo,v_facReendrTimes,v_facRecTimes,v_facMessageCode,v_facMessage);
          if v_facMessageCode<>'0' then
            select '*' into v_char from dual where 1=2;
          end if;
       end if;
       --�۳�ԭ��
       select sum(risum/exchrate),sum(grsprem/exchrate),sum(netPrem/exchrate),sum(chgrisum/exchrate),sum(chggrsprem/exchrate),sum(chgnetprem/exchrate)
        into v_facAmount,v_facGrsPrem,v_facNetPrem,v_facChgRISum,v_facChgGrsPrem,v_facChgNetPrem
        from REINS_REENDR_FAC
        where repolicyno=v_facRepolicyNo and rectimes=v_facRecTimes and reendortimes=v_facReendrTimes;
        v_RISum:=v_RISum+v_facAmount;
        v_GrsPrem:=v_GrsPrem+v_facGrsPrem;
        v_NetPrem:=v_NetPrem+v_facNetPrem;
        v_chgRISum:=v_chgRISum+v_facChgRISum;
        v_chgGrsPrem:=v_chgGrsPrem+v_facChgGrsPrem;
        v_chgNetPrem:=v_chgNetPrem+v_facChgNetPrem;
     end if;
    --���ȵ���������о������ȵ�����������
    if rec_edr.sharerate=100-v_nfac_sharerate or v_facAmount<>0 then
     if abs(rec_edr.pml*(100-v_nfac_sharerate)/100-v_RISum)>=0.01 or
       abs(rec_edr.grsprem*(100-v_nfac_sharerate)/100-v_GrsPrem)>=0.01 or
       abs(rec_edr.NetPrem*(100-v_nfac_sharerate)/100-v_NetPrem)>=0.01 or
       abs(rec_edr.ChgPml*(100-v_nfac_sharerate)/100-v_ChgRISum)>=0.01 or
       abs(rec_edr.Chggrsprem*(100-v_nfac_sharerate)/100-v_ChgGrsPrem)>=0.01 or
       abs(rec_edr.ChgNetPrem*(100-v_nfac_sharerate)/100-v_ChgNetPrem)>=0.01   then
         if rec_edr.reinstype='0'  then--��Լ����
           if v_findretnind='Y' then
             update  REINS_REENDR_SHARE set RISum =RISum+(rec_edr.pml*(100-v_nfac_sharerate)/100-v_RISum),
                                   grsprem=grsprem+(rec_edr.grsprem*(100-v_nfac_sharerate)/100-v_GrsPrem),
                                   NetPrem=NetPrem+(rec_edr.NetPrem*(100-v_nfac_sharerate)/100-v_NetPrem),
                                   Gportfprem=Gportfprem+(rec_edr.grsprem*(100-v_nfac_sharerate)/100-v_GrsPrem),
                                   Nportfprem=Nportfprem+(rec_edr.NetPrem*(100-v_nfac_sharerate)/100-v_NetPrem),
                                   ChgRISum =ChgRISum+(rec_edr.ChgPml*(100-v_nfac_sharerate)/100-v_ChgRISum),
                                   Chggrsprem=Chggrsprem+(rec_edr.Chggrsprem*(100-v_nfac_sharerate)/100-v_ChgGrsPrem),
                                   ChgNetPrem=ChgNetPrem+(rec_edr.ChgNetPrem*(100-v_nfac_sharerate)/100-v_ChgNetPrem),
                                   date_updated = sysdate
             where repolicyno=p_repolicyno
               and reendortimes=p_reendortimes
               and RecTimes=p_Rectimes
               and ttytype='81';
            else
               update  REINS_REENDR_SHARE set RISum =RISum+(rec_edr.pml*(100-v_nfac_sharerate)/100-v_RISum)*exchrate,
                                       grsprem=grsprem+(rec_edr.grsprem*(100-v_nfac_sharerate)/100-v_GrsPrem)*exchrate,
                                       NetPrem=NetPrem+(rec_edr.NetPrem*(100-v_nfac_sharerate)/100-v_NetPrem)*exchrate,
                                       Gportfprem=Gportfprem+(rec_edr.grsprem*(100-v_nfac_sharerate)/100-v_GrsPrem)*exchrate,
                                       Nportfprem=Nportfprem+(rec_edr.NetPrem*(100-v_nfac_sharerate)/100-v_NetPrem)*exchrate,
                                       ChgRISum =ChgRISum+(rec_edr.ChgPml*(100-v_nfac_sharerate)/100-v_ChgRISum)*exchrate,
                                       Chggrsprem=Chggrsprem+(rec_edr.Chggrsprem*(100-v_nfac_sharerate)/100-v_ChgGrsPrem)*exchrate,
                                       ChgNetPrem=ChgNetPrem+(rec_edr.ChgNetPrem*(100-v_nfac_sharerate)/100-v_ChgNetPrem)*exchrate,
                                       date_updated = sysdate
               where repolicyno=p_repolicyno
                 and reendortimes=p_reendortimes
                 and RecTimes=p_Rectimes
                 and risum = (select max(risum) from REINS_REENDR_SHARE where repolicyno=p_repolicyno and reendortimes=p_reendortimes and rectimes=p_RecTimes and rownum=1)
                 and rownum=1;
            end if;
            update REINS_REENDR_SHARE set chgRIPrem=decode(netind,'0',chgGrsPrem,chgNetPrem),date_updated = sysdate
             where repolicyno=p_repolicyno
               and reendortimes=p_reendortimes
               and RecTimes=p_Rectimes;
          end if;
      end if;
     end if;
   end if;
 /*if abs(rec_edr.pml*rec_edr.sharerate/100-v_RISum)>=0.01  then
       if rec_edr.reinstype='0'  then--��Լ����
         if v_findretnind='Y' then
           update  REINS_REENDR_SHARE set RISum =RISum+(rec_edr.pml*rec_edr.sharerate/100-v_RISum)
           where repolicyno=p_repolicyno
             and reendortimes=p_reendortimes
             and RecTimes=p_Rectimes
             and ttytype='81';
      else
         update  REINS_REENDR_SHARE set RISum =RISum+(rec_edr.pml*rec_edr.sharerate/100-v_RISum)*exchrate
         where repolicyno=p_repolicyno
           and reendortimes=p_reendortimes
           and RecTimes=p_Rectimes
           and rownum=1;

      end if;
    else  -- �ٷֵ���
       insert into REINS_REENDR_SHARE
        (REPOLICYNO     ,
         REendortimes ,
         RECTIMES       ,
         TTYID          ,
         RISKCODE       ,
         REFERNO        ,
         TTYCODE        ,
         UWYEAR         ,
         PCIND          ,
         POLICYNO       ,
         DANGERUNITNO   ,
         DANGERCODE     ,
         DANGERTYPE     ,
         COMCODE        ,
         RERISKCODE     ,
         businessind,
         PLYYEAR        ,
         STATCLASS      ,
         TTYTYPE        ,
         SECTNO         ,
         startdate      ,
         ENDDATE        ,
         REstartdate    ,
         REENDDATE      ,
         SHARERATE      ,
         ORICURR        ,
         RICURR         ,
         EXCHRATE       ,
         RISUM          ,
         GEARNEDPREM    ,
         GPORTFPREM     ,
         NEARNEDPREM    ,
         NPORTFPREM     ,
         GRSPREM        ,
         NETPREM        ,
         RIPREM         ,
         CHGRISUM       ,
         CHGGRSPREM     ,
         CHGNETPREM     ,
         CHGRIPREM      ,
         NETIND         ,
         ACCCHANNEL,
         planCode)
    select
        REPOLICYNO    ,
        REendortimes ,
        RECTIMES       ,
        get_affi_ttyid(to_char(startdate,'YYYY')),
        RISKCODE ,
        null, --�ο���
        substr(get_affi_ttyid(to_char(startdate,'YYYY')),1,5),
        UWYEAR,
        PCIND ,
        POLICYNO,
        DANGERUNITNO,
        DANGERCODE ,
        DANGERTYPE  ,
        COMCODE     ,
        RERISKCODE  ,
        businessind,
        to_number(to_char(rec_edr.startdate,'yyyy')) ,
        '1' ,
        '82'  ,
         '0'  ,
         startdate ,
         ENDDATE   ,
         REstartdate ,
         REENDDATE ,
         0,
         oricurr ,
         RICURR ,
         EXCHRATE    ,
         rec_edr.pml*rec_edr.sharerate/100-v_RISum       ,
         0 ,
         0  ,
         0 ,
         0  ,
         0,--rec_edr.grsprem*rec_edr.sharerate/100-v_GrsPrem ,
         0,--rec_edr.NetPrem*rec_edr.sharerate/100-v_NetPrem,
         0,--rec_edr.NetPrem*rec_edr.sharerate/100-v_NetPrem,
         0,--rec_edr.ChgPml*rec_edr.sharerate/100-v_ChgRISum,
         0,--rec_edr.Chggrsprem*rec_edr.sharerate/100-v_ChgGrsPrem,
         0,--rec_edr.ChgNetPrem*rec_edr.sharerate/100-v_ChgNetPrem,
         0,--rec_edr.Chggrsprem*rec_edr.sharerate/100-v_ChgGrsPrem  ,
         '0',
         ACCCHANNEL,
         planCode
     from REINS_REENDR_SHARE
      where repolicyno=p_repolicyno
        and reendortimes=p_reendortimes
        and RecTimes=p_Rectimes;
   end if;
   update  REINS_REENDR_SHARE set RIprem =decode(netInd,'0',Grsprem,Netprem),
                                 ChgRIprem =decode(netInd,'0',ChgGrsprem,ChgNetprem)
        where repolicyno=p_repolicyno
          and reendortimes=p_reendortimes
          and RecTimes=p_Rectimes;
           update  REINS_REENDR_SHARE set RIprem =decode(RIPRem,0,decode(netInd,'0',Grsprem,Netprem),Riprem),
                             ChgRIprem =decode(chgRIPRem,0,decode(netInd,'0',ChgGrsprem,ChgNetprem),chgRiPrem)
        where repolicyno=p_repolicyno
          and reendortimes=p_reendortimes
          and RecTimes=p_Rectimes;
     end if;*/
end  crt_edr_abs;

/*����REINS_RECLAIM_SHARE���������ṹ���Ƶ�p_share����ֵ����REINS_RECLAIM_SHARE*/
procedure crt_clm_abs(p_reclaimno  in REINS_RECLAIM.reclaimno%type,
                      p_RecTimes   in REINS_REPOLICY.RecTimes%type,
                      p_share in  arr_danger_share ) is
cursor cur_clm is
   select * from REINS_RECLAIM
    where reclaimno=p_reclaimno
      and RecTimes=p_Rectimes;
rec_clm cur_clm%rowtype;
n_Seq number(4);
v_char char(1);
v_PaidSum REINS_RECLAIM.paidsum%type:=0;
v_ttyType REINS_TTY_TABLE.ttytype%type;
v_findretnind varchar2(1);
v_tot_rate REINS_RECLAIM_SHARE.sharerate%type:=0;
v_fac_reclaimno REINS_RECLAIM.reclaimno%type;
v_fac_rectimes REINS_RECLAIM.rectimes%type;
v_fac_sum REINS_RECLAIM.paidsum%type:=0;
v_status REINS_RECLAIM.status%type;
v_message_code varchar2(10);
v_message_desc varchar2(255);
v_reten_ttycode varchar2(9);
begin
 open cur_clm;
   fetch cur_clm into rec_clm;
 close cur_clm;

 if p_share.count=0 then
   g_errcode:='B3104';
   g_errmsg :='û���ҵ��ֳ�����';
   select '1' into v_char from dual where 1=2;
 end if;
 select ttycode into v_reten_ttycode from REINS_TTY_TABLE where ttytype='82';
 for n_Seq in 1..p_share.count loop
   v_PaidSum:=v_PaidSum+p_share(n_seq).PaidSum/p_share(n_seq).EXCHRATE;
   v_tot_rate:=v_tot_rate+p_share(n_seq).SHARERATE;
   select b.ttytype into v_ttyType
   from REINS_TREATY a,REINS_TTY_TABLE b
   where a.ttycode=b.ttycode
     and a.ttyid=p_share(n_seq).ttyid;
     if v_ttyType='81' then
       v_findretnind:='Y';
     end if;
   g_errmsg := '���ɷֳ���ʧ��'||p_share(n_seq).TTYID;
   insert into REINS_RECLAIM_SHARE(
           RECLAIMNO  ,
           RECTIMES   ,
           TTYID      ,
           PCIND      ,
           RISKCODE   ,
           businessind,
           RERISKCODE ,
           CLAIMNO    ,
           LOSSSEQNO  ,
           PAYNO      ,
           COMCODE    ,
           DAMAGEDATE ,
           DANGERTYPE ,
           DANGERCODE ,
           SECTNO     ,
           STATCLASS  ,
           TTYTYPE    ,
           TTYCODE    ,
           UWYEAR     ,
           SHARERATE  ,
           ORICURR    ,
           RICURR     ,
           EXCHRATE   ,
           PAIDSUM  ,
           REPORTDATE  ,
           accchannel,
           planCode,
           date_created,
           date_updated)
       select
           rec_clm.RECLAIMNO      ,
           rec_clm.RECTIMES       ,
           p_share(n_seq).TTYID   ,
           p_share(n_seq).PCIND   ,
           rec_clm.RISKCODE       ,
           rec_clm.businessind,
           rec_clm.RERISKCODE     ,
           rec_clm.CLAIMNO        ,
           rec_clm.LOSSSEQNO      ,
           rec_clm.PAYNO          ,
           rec_clm.COMCODE        ,
           rec_clm.DAMAGEDATE     ,
           rec_clm.DANGERTYPE     ,
           rec_clm.DANGERCODE     ,
           p_share(n_seq).SECTNO  ,
           b.STATCLASS            ,
           b.TTYTYPE              ,
           b.TTYCODE              ,
           a.UWYEAR               ,
           nvl(p_share(n_seq).SHARERATE, 0) ,
           rec_clm.currency ORICURR ,
           p_share(n_seq).RICURR    ,
           p_share(n_seq).EXCHRATE  ,
           nvl(p_share(n_seq).PAIDSUM, 0) ,
           sysdate,
           rec_clm.ACCCHANNEL,
           rec_clm.PlanCode,
           sysdate,
           sysdate
        from REINS_TREATY a,REINS_TTY_TABLE b
       where a.ttycode=b.ttycode
         and a.ttyid=p_share(n_seq).ttyid;
   if rec_clm.reinstype='2' then
     insert into REINS_RECLAIM_SHARE(
         RECLAIMNO  ,
         RECTIMES   ,
         TTYID      ,
         PCIND      ,
         RISKCODE   ,
         businessind,
         RERISKCODE ,
         CLAIMNO    ,
         LOSSSEQNO  ,
         PAYNO      ,
         COMCODE    ,
         DAMAGEDATE ,
         DANGERTYPE ,
         DANGERCODE ,
         SECTNO     ,
         STATCLASS  ,
         TTYTYPE    ,
         TTYCODE    ,
         UWYEAR     ,
         SHARERATE  ,
         ORICURR    ,
         RICURR     ,
         EXCHRATE   ,
         PAIDSUM  ,
         REPORTDATE  ,
         accchannel,
         planCode,
         date_created,
         date_updated)
     select
         rec_clm.RECLAIMNO      ,
         rec_clm.RECTIMES       ,
         a.TTYID   ,
         p_share(n_seq).PCIND   ,
         rec_clm.RISKCODE       ,
         rec_clm.businessind,
         rec_clm.RERISKCODE     ,
         rec_clm.CLAIMNO        ,
         rec_clm.LOSSSEQNO      ,
         rec_clm.PAYNO          ,
         rec_clm.COMCODE        ,
         rec_clm.DAMAGEDATE     ,
         rec_clm.DANGERTYPE     ,
         rec_clm.DANGERCODE     ,
         p_share(n_seq).SECTNO  ,
         b.STATCLASS            ,
         b.TTYTYPE              ,
         b.TTYCODE              ,
         a.UWYEAR               ,
         nvl(p_share(n_seq).SHARERATE, 0) * -1 ,
         rec_clm.currency ORICURR ,
         p_share(n_seq).RICURR    ,
         p_share(n_seq).EXCHRATE  ,
         nvl(p_share(n_seq).PAIDSUM, 0) * -1,
         sysdate,
         rec_clm.ACCCHANNEL,
         rec_clm.PlanCode,
         sysdate,
         sysdate
      from REINS_TREATY a,REINS_TTY_TABLE b
     where a.ttycode=b.ttycode
       and a.ttyid=v_reten_ttycode||substr(p_share(1).TTYID,6);
   end if;
 end loop;

 --�ٷ�̯�ض��
 if rec_clm.reinstype='0' then
   begin
     select reclaimno,rectimes,status into v_fac_reclaimno,v_fac_rectimes,v_status
       from REINS_RECLAIM
      where payno=rec_clm.payno
        and lossseqno=rec_clm.lossseqno
        and dangerunitno=rec_clm.dangerunitno
        and riskcode=rec_clm.riskcode
        and reriskcode=rec_clm.reriskcode
        and reinstype='1'
        and status in ('0','1','2');
   exception when others then
     v_fac_sum:=-1;
   end;
   if v_fac_sum<>-1 then
     g_errmsg := '��������ٷַ��ⰸ';
     if v_status<>'1' then
        fac_clm_cal(v_fac_reclaimno,v_fac_recTimes,v_message_code,v_message_desc);
        if v_message_code!='0' then
          select '*' into v_char from dual where 1=2;
        end if;
     end if;
     select sum(paidsum/exchrate) into v_fac_sum
      from REINS_RECLAIM_SHARE
       where reclaimno=v_fac_reclaimno and rectimes=v_fac_rectimes;
   else
     v_fac_sum := 0;
   end if;
   v_PaidSum:=v_PaidSum+v_fac_sum;
 end if;

  --���ȵ���������о������ȵ�����������
  if v_tot_rate=100 or v_fac_sum<>0 then
    if abs(rec_clm.paidsum-v_PaidSum)>=0.01 then
        if v_findretnind='Y' then
          update  REINS_RECLAIM_SHARE set PaidSum =PaidSum+(rec_clm.Paidsum-v_PaidSum),date_updated=sysdate
           where  reclaimno=p_reclaimno and RecTimes=p_Rectimes  and ttytype='81'
            and rownum=1;
        else
          update  REINS_RECLAIM_SHARE set PaidSum =PaidSum+(rec_clm.Paidsum-v_PaidSum)*exchrate,date_updated=sysdate
           where  reclaimno=p_reclaimno and RecTimes=p_Rectimes
            and Paidsum = (select max(paidsum) from REINS_RECLAIM_SHARE
                         where reclaimno=p_reclaimno and RecTimes=p_Rectimes and rownum=1)
            and rownum=1;
        end if;
    end if;
  end if;
 end crt_clm_abs;

/*����REINS_OS_RECLAIM_SHARE���������ṹ���Ƶ�p_share����ֵ����REINS_OS_RECLAIM_SHARE*/
procedure crt_OSclm_abs(p_OSreclaimno  in REINS_OS_RECLAIM.OSreclaimno%type,
                        p_share        in arr_danger_share ) is
cursor cur_clm is
   select * from REINS_OS_RECLAIM
    where OSreclaimno=p_OSreclaimno ;
rec_clm cur_clm%rowtype;
n_Seq number(4);
v_char char(1);
v_ossum number(16,2):=0;
v_findretnind varchar2(1);
v_ttyType REINS_TTY_TABLE.ttytype%type;
v_tot_rate REINS_RECLAIM_SHARE.sharerate%type:=0;
v_fac_osreclaimno REINS_OS_RECLAIM.osreclaimno%type;
v_fac_sum REINS_RECLAIM.paidsum%type:=0;
v_status REINS_RECLAIM.status%type;
v_email_flag varchar2(1):='0'; --�Ƿ��Ѿ���PUB_EMAIL_TASK���ֵ
v_plaind varchar2(1);
v_prelossamount number(16,2);
v_taskid PUB_EMAIL_TASK.taskid%type;
v_reten_ttycode varchar2(9);
v_email_sender varchar2(50);
v_message_code varchar2(10);
v_message_desc varchar2(255);
begin
 open cur_clm;
  fetch cur_clm into rec_clm;
 close cur_clm;
 if p_share.count=0 then
    g_errmsg := 'û���ҵ��ֳ�����';
    select '1' into v_char from dual where 1=2;
 end if;

 select ttycode into v_reten_ttycode from REINS_TTY_TABLE where ttytype='82';
 for n_Seq in 1..p_share.count loop
   v_ossum:=v_ossum+p_share(n_seq).PAIDSUM/p_share(n_seq).EXCHRATE;
   v_tot_rate:=v_tot_rate+p_share(n_seq).SHARERATE;
   select b.ttytype into v_ttyType
     from REINS_TREATY a,REINS_TTY_TABLE b
     where a.ttycode=b.ttycode
       and a.ttyid=p_share(n_seq).ttyid;
       if v_ttyType='81' then
         v_findretnind:='Y';
       end if;
       --����������Լ��������֪ͨ����PUB_EMAIL_TASK�������
       if v_ttyType not in ('81','82','91','92') and v_email_flag='0' then
         g_errmsg := '����Email�Զ���ʾ��Ϣ����';
         --ȡ�����taskid
         select trunc(dbms_random.value(100000000000000, 999999999999999))
           into v_taskid
           from dual;
         select nvl(plaind,'0'), nvl(prelossamount,0)
           into v_plaind, v_prelossamount
           from REINS_TREATY
          where ttyid = p_share(n_seq).TTYID;
          --ע���ʼ����ͳ���
          /*if v_prelossamount<>0 then
             if (v_plaind='0' and v_prelossamount<nvl(p_share(n_seq).PAIDSUM,0)) or
                (v_plaind='1' and v_prelossamount<nvl(rec_clm.ossum,0)) then
               v_email_flag:='1';
               begin
                 select a.EMAILADDRESS into v_email_sender from PUB_EMAIL a where a.usertype='0' and rownum=1;
               exception when others then
                 v_email_sender:='send@sinosoft.com.cn';
               end;
               insert into PUB_EMAIL_TASK(
                 taskid,
                 sendemail,
                 acceptemail,
                 emailtitle,
                 emailcontents,
                 createdate,
                 status)
               select
                  v_taskid,
                  v_email_sender,
                  emailAddress,
                  '������ '||rec_clm.claimno||' ���ں�Լ��������֪ͨ��',
                  '���ã�    ������'||rec_clm.policyno||' ���գ����ں�Լ��������֪ͨ�飬��Ӧ�������ţ�' ||rec_clm.claimno,
                  sysdate,
                  '0'
                from PUB_EMAIL
               where usertype='2'
                 and rownum=1;
             end if;
          end if;*/
       end if;

 g_errmsg := '����δ���ֳ���';
 insert into REINS_OS_RECLAIM_SHARE(
       OSRECLAIMNO  ,
       TTYID      ,
       PCIND      ,
       RISKCODE   ,
       businessind,
       RERISKCODE ,
       CLAIMNO    ,
       COMCODE    ,
       DAMAGEDATE ,
       DANGERTYPE ,
       DANGERCODE ,
       SECTNO     ,
       STATCLASS  ,
       TTYTYPE    ,
       TTYCODE    ,
       UWYEAR     ,
       SHARERATE  ,
       ORICURR    ,
       RICURR     ,
       EXCHRATE   ,
       OSSUM ,
       ReportDate,
       ACCCHANNEL,
       PlanCode,
       date_created,
       date_updated)
   select
       rec_clm.OSRECLAIMNO      ,
       p_share(n_seq).TTYID   ,
       p_share(n_seq).PCIND   ,
       rec_clm.RISKCODE       ,
       rec_clm.businessind,
       rec_clm.RERISKCODE     ,
       rec_clm.CLAIMNO        ,
       rec_clm.COMCODE        ,
       rec_clm.DAMAGEDATE     ,
       rec_clm.DANGERTYPE     ,
       rec_clm.DANGERCODE     ,
       p_share(n_seq).SECTNO  ,
       b.STATCLASS            ,
       b.TTYTYPE              ,
       b.TTYCODE              ,
       a.UWYEAR               ,
       nvl(p_share(n_seq).SHARERATE,0) ,
       rec_clm.currency ORICURR ,
       p_share(n_seq).RICURR    ,
       p_share(n_seq).EXCHRATE  ,
       nvl(p_share(n_seq).PAIDSUM,0)   ,
       rec_clm.reportdate,
       rec_clm.ACCCHANNEL,
       rec_clm.PlanCode,
       sysdate,
       sysdate
    from REINS_TREATY a,REINS_TTY_TABLE b
   where a.ttycode=b.ttycode
     and a.ttyid=p_share(n_seq).ttyid;
   if rec_clm.reinstype='2' then
     insert into REINS_OS_RECLAIM_SHARE(
       OSRECLAIMNO  ,
       TTYID      ,
       PCIND      ,
       RISKCODE   ,
       businessind,
       RERISKCODE ,
       CLAIMNO    ,
       COMCODE    ,
       DAMAGEDATE ,
       DANGERTYPE ,
       DANGERCODE ,
       SECTNO     ,
       STATCLASS  ,
       TTYTYPE    ,
       TTYCODE    ,
       UWYEAR     ,
       SHARERATE  ,
       ORICURR    ,
       RICURR     ,
       EXCHRATE   ,
       OSSUM ,
       ReportDate,
       ACCCHANNEL,
       PlanCode,
       date_created,
       date_updated)
     select
       rec_clm.OSRECLAIMNO      ,
       a.TTYID   ,
       p_share(n_seq).PCIND   ,
       rec_clm.RISKCODE       ,
       rec_clm.businessind,
       rec_clm.RERISKCODE     ,
       rec_clm.CLAIMNO        ,
       rec_clm.COMCODE        ,
       rec_clm.DAMAGEDATE     ,
       rec_clm.DANGERTYPE     ,
       rec_clm.DANGERCODE     ,
       p_share(n_seq).SECTNO  ,
       b.STATCLASS            ,
       b.TTYTYPE              ,
       b.TTYCODE              ,
       a.UWYEAR               ,
       nvl(p_share(n_seq).SHARERATE,0) * -1,
       rec_clm.currency ORICURR ,
       p_share(n_seq).RICURR    ,
       p_share(n_seq).EXCHRATE  ,
       nvl(p_share(n_seq).PAIDSUM,0) * -1  ,
       rec_clm.reportdate,
       rec_clm.ACCCHANNEL,
       rec_clm.PlanCode,
       sysdate,
       sysdate
    from REINS_TREATY a,REINS_TTY_TABLE b
   where a.ttycode=b.ttycode
     and a.ttyid=v_reten_ttycode||substr(p_share(1).TTYID,6);
   end if;
 end loop;

 --�ٷ�̯�ض��
 if rec_clm.reinstype='0' then
   begin
     select osreclaimno,status into v_fac_osreclaimno,v_status
       from REINS_OS_RECLAIM
      where claimno=rec_clm.claimno
        and dangerunitno=rec_clm.dangerunitno
        and riskcode=rec_clm.riskcode
        and reriskcode=rec_clm.reriskcode
        and reinstype='1'
        and status in ('0','1','2');
   exception when others then
      v_fac_sum:=-1;
   end;
   if v_fac_sum<>-1 then
      g_errmsg := '��������ٷ�δ�����ⰸ';
      if v_status<>'1' then
        fac_OSclm_cal(v_fac_osreclaimno,v_message_code,v_message_desc);
        if v_message_code<>'0' then
          select '*' into v_char from dual where 1=2;
        end if;
      end if;
      select sum(ossum/exchrate) into v_fac_sum from REINS_OS_RECLAIM_SHARE where osreclaimno=v_fac_osreclaimno;
    else
       v_fac_sum:=0;
    end if;
    v_ossum:=v_ossum+v_fac_sum;
 end if;

 --���ȵ���������о������ȵ���������
 if v_tot_rate=100 or v_fac_sum<>0 then
   if abs(rec_clm.OSsum-v_ossum)>=0.01 then
        if v_findretnind='Y' then
         update  REINS_OS_RECLAIM_SHARE set OSSum =OSSum+(rec_clm.OSsum-v_ossum),date_updated = sysdate
          where  OSreclaimno=p_OSreclaimno  and ttytype='81';
         else
         update  REINS_OS_RECLAIM_SHARE set OSSum =OSSum+(rec_clm.OSsum-v_ossum),date_updated = sysdate
          where  OSreclaimno=p_OSreclaimno and ossum = (select max(ossum) from REINS_OS_RECLAIM_SHARE where osreclaimno = p_OSreclaimno and rownum=1) and rownum=1;
        end if;
   end if;
 end if;

 end crt_OSclm_abs;

/*ȡ�ϴηֱ������Ϊ�˼����������ڱ���*/
procedure get_last_re(p_repolicyno   in REINS_REPOLICY.repolicyno%type,
                      p_reendortimes in REINS_REPOLICY.reendortimes%type,
                      p_termi_date   in date ,
                      p_arr_re_abs   in out arr_re_abs
                       ) is
  cursor cur_last_re is
     select a.TtyID,a.ttycode,a.uwyear,a.sectno,a.ttytype,a.statclass,a.PCInd,a.netind,a.exchrate,
            a.ShareRate,a.Restartdate,a.ReEndDATE,b.enddate,a.RICurr, a.RISum,a.GrsPrem,a.NetPrem,
            a.RIPrem,0 GEarnedPrem,0 NEarnedPrem,a.GrsPrem GPortfPrem, a.NetPrem NPortfPrem
     from REINS_REPLY_SHARE a,REINS_REPOLICY b
    where a.RepolicyNo=b.RepolicyNo
      and a.RecTimes=b.RecTimes
      and b.Status='1'
      and b.RepolicyNo=p_repolicyno
      and p_Reendortimes='000'
  union
    select a.TtyID,a.ttyCode,a.uwyear,a.sectno,a.ttytype,a.statclass,a.PCInd,a.netind,a.exchrate,
           a.ShareRate,a.Restartdate,a.ReEndDATE,b.enddate,a.RICurr,a.RISum,a.GrsPrem,a.NetPrem,
           a.RIPrem, a.GEarnedPrem, a.NEarnedPrem,a.GPortfPrem, a.NPortfPrem
     from REINS_REENDR_SHARE a,REINS_REENDOR b
    where a.RepolicyNo=b.RepolicyNo
      and a.reendortimes=b.reendortimes
      and a.RecTimes=b.RecTimes
      and b.Status='1'
      and b.RepolicyNo=p_repolicyno
      and p_Reendortimes<>'000'
      and b.Reendortimes=p_Reendortimes;
   rec_last_re  cur_last_re%rowtype;
   n_cnt number(4):=1;
   v_termi_rate REINS_REPLY_SHARE.ExchRate%type;
   v_last_shareRate REINS_REPLY_SHARE.Sharerate%type;

begin

    n_cnt:=1;
    for rec_last_re in cur_last_re loop
      p_arr_re_abs(n_cnt).ttyid:=rec_last_re.ttyid;
      p_arr_re_abs(n_cnt).ttycode:=rec_last_re.ttycode;
      p_arr_re_abs(n_cnt).Uwyear:=rec_last_re.Uwyear;
      p_arr_re_abs(n_cnt).sectno:=rec_last_re.sectno;
      p_arr_re_abs(n_cnt).ttytype:=rec_last_re.ttytype;
      p_arr_re_abs(n_cnt).statclass:=rec_last_re.statclass;
      p_arr_re_abs(n_cnt).PCInd:=rec_last_re.PCInd;
      p_arr_re_abs(n_cnt).NetInd:=rec_last_re.NetInd;
      p_arr_re_abs(n_cnt).ricurr:=rec_last_re.ricurr;
      p_arr_re_abs(n_cnt).exchrate:=rec_last_re.exchrate;
      p_arr_re_abs(n_cnt).sharerate:=rec_last_re.sharerate;
      v_termi_rate := 0;
      if trunc(p_termi_date)-trunc(rec_last_re.restartdate)<0 then
        v_termi_rate:=0;
      else
--        v_termi_rate:=(trunc(p_termi_date)-trunc(rec_last_re.restartdate))/(trunc(rec_last_re.enddate)+1-trunc(rec_last_re.restartdate));
          v_termi_rate:=(trunc(p_termi_date)-trunc(rec_last_re.restartdate))/(trunc(rec_last_re.enddate)-trunc(rec_last_re.restartdate)+1);  --modify  by  wuwp 20160721
      end if;
      p_arr_re_abs(n_cnt).GPortfPrem:=rec_last_re.GPortfPrem;
      p_arr_re_abs(n_cnt).NPortfPrem:=rec_last_re.NPortfPrem;
      p_arr_re_abs(n_cnt).GEarnedPrem:=rec_last_re.GEarnedPrem+rec_last_re.GPortfPrem*v_termi_rate;
      p_arr_re_abs(n_cnt).NEarnedPrem:=rec_last_re.NEarnedPrem+rec_last_re.NPortfPrem*v_termi_rate;
      p_arr_re_abs(n_cnt).GrsPrem:=rec_last_re.GrsPrem;
      p_arr_re_abs(n_cnt).NetPrem:=rec_last_re.NetPrem;
      p_arr_re_abs(n_cnt).risum:=rec_last_re.risum;
      n_cnt:=n_cnt+1;
   end loop;
   --���ڱ��ѵ����ϴ����ڱ���/�ֱ���ռ��
   /*if(p_Reendortimes='000') then
     select shareRate into v_last_shareRate from REINS_REPOLICY where Status='1' and RepolicyNo=p_repolicyno;
   else
     select shareRate into v_last_shareRate from REINS_REENDOR where Status='1' and RepolicyNo=p_repolicyno and Reendortimes=p_Reendortimes;
   end if;
   p_GrsLastEarned:=p_GrsLastEarned*100/v_last_shareRate;
   p_NetLastEarned:=p_NetLastEarned*100/v_last_shareRate;*/
end get_last_re;

/*ȡ�ϴηֱ�δ���ڱ���*/
procedure get_last_ply_earned(p_repolicyno   in REINS_REPOLICY.repolicyno%type,
                              p_reendortimes in REINS_REPOLICY.reendortimes%type,
                              p_termi_date   in date ,
                              p_GRSlastearned   out REINS_REPOLICY.pml%type,
                              p_Netlastearned   out REINS_REPOLICY.pml%type
                       ) is
  /*cursor cur_sumfee is
     select a.Restartdate,a.ReEndDATE,a.enddate,a.startdate,a.Currency,a.NetPrem*(trunc(p_termi_date)-trunc(startdate))/(trunc(enddate)-trunc(startdate)) as netprem, a.GrsPrem*(trunc(p_termi_date)-trunc(startdate))/(trunc(enddate)-trunc(startdate)+1) as grsprem,a.sharerate
     from REINS_REPOLICY a
    where status<>'5'
      and a.RepolicyNo=p_repolicyno
  union all
    select a.Restartdate,a.ReEndDATE,a.enddate,a.startdate,a.currency,a.chgNetPrem*(trunc(p_termi_date)-trunc(validdate))/(trunc(enddate)-trunc(validdate)) as netprem, a.chgGrsPrem*(trunc(p_termi_date)-trunc(validdate))/(trunc(enddate)-trunc(validdate)+1) as grsprem,a.sharerate
     from REINS_REENDOR a
    where a.RepolicyNo=p_repolicyno
      and a.reendortimes<>p_reendortimes
      and status<>'5' ;*/
  cursor cur_sumfee is
     select a.Restartdate,a.ReEndDATE,a.enddate,a.startdate,a.Currency,a.NetPrem*(trunc(p_termi_date)-trunc(startdate))/(trunc(enddate)-trunc(startdate)+1) as netprem, a.GrsPrem*(trunc(p_termi_date)-trunc(startdate))/(trunc(enddate)-trunc(startdate)+1) as grsprem,a.sharerate   --modify  by  wuwp 20160721
     from REINS_REPOLICY a
    where status<>'5'
      and a.RepolicyNo=p_repolicyno
      and a.restartdate<p_termi_date
  union all
    select a.Restartdate,a.ReEndDATE,a.enddate,a.startdate,a.currency,a.chgNetPrem*(trunc(p_termi_date)-trunc(validdate))/(trunc(enddate)-trunc(validdate)+1) as netprem, a.chgGrsPrem*(trunc(p_termi_date)-trunc(validdate))/(trunc(enddate)-trunc(validdate)+1) as grsprem,a.sharerate   --modify  by  wuwp 20160721
     from REINS_REENDOR a
    where a.RepolicyNo=p_repolicyno
      and a.reendortimes<>p_reendortimes
      and status<>'5'
      and a.restartdate<p_termi_date ;

   rec_sumfee  cur_sumfee%rowtype;

/*v_curr REINS_REPOLICY.Currency%type;*/
begin
   p_GRSlastearned:=0;
   p_NetLastEarned:=0;
  /* select currency into v_curr
     from REINS_REENDOR
     where repolicyno=p_repolicyno
       and reendortimes=p_reendortimes
       and status not in ('3','4','5');*/
   for rec_sumfee in cur_sumfee loop
      p_GRSlastearned:=p_GRSlastearned + rec_sumfee.grsprem  ;
      p_NetLastEarned:=p_NetLastEarned + rec_sumfee.Netprem  ;
   end loop;
end get_last_ply_earned;

/*ȡ�ϴηֱ�����״̬������ϴ�ÿ����ɹ�������Ҳ���ܼ���*/
function get_last_status(p_repolicyno   REINS_REPOLICY.repolicyno%type,
                         p_reendortimes REINS_REPOLICY.reendortimes%type)
return REINS_REENDOR.status%type is

cursor cur_status is
  select status from REINS_REPOLICY
   where RepolicyNo=p_repolicyno
     and p_Reendortimes='000'
     --and status='1'
     and status = '1'
    union
  select status from REINS_REENDOR
   where RepolicyNo=p_repolicyno
     and p_Reendortimes<>'000'
     and reendortimes=p_Reendortimes
     --and status='1' ;
     and status = '1';
v_status REINS_REPOLICY.status%type;
begin

   if p_reendortimes='-1' then
     v_status:='1';
   else
     open cur_status;
      fetch cur_status into v_status;
      if cur_status%notfound then
       v_Status:='0';
      else
       v_Status:='1';
      end if;
     close cur_status;
    end if;

   return v_status;

end get_last_status;

/*�����·ֱ�������ϴηֱ�����Ƚϣ�����������ķֳ���ֵ*/
procedure jion_re_rslt(p_LastEdrAbs in  arr_re_abs,
                       p_unit_para  in policy_unit_para, -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼��
                       p_newedrabs in out arr_re_abs )is
   n_seq1 number(3);
   n_seq2 number(3);
   n_newcount number(3);
   FindFlag varchar2(1);
   -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼�� begin
   v_Ricommrateadjind REINS_TTY_SECT.Ricommrateadjind%type;
   v_char varchar2(1);
   -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼�� end
   --����Ӷ��
   -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼�� begin
  cursor cur_ttySectReins(p_ttyid varchar2,p_sectno varchar2) is
  select b.sharerate, a.rcrate ,c.ricommrateadjind
    from REINS_TTY_SECT_REINS a, REINS_TTY_REINS b, REINS_TTY_SECT c
   where a.ttyid = b.ttyid
     and a.ttyid = p_ttyid
     and a.sectno = p_sectno
     and a.ttyid = c.ttyid
     and a.sectno = c.sectno
     and a.reinsCode = b.ReinsCode
     and nvl(a.brokercode,'*')=nvl(b.brokercode,'*');
  rec_ttySectReins cur_ttySectReins%rowtype;

  cursor cur_PolicyPlanAdj(v_policyno Reins_Policy_Plan_Adj.Policyno%type,
                           v_dangerunitno Reins_Policy_Plan_Adj.Dangerunitno%type,
                           v_reriskcode Reins_Policy_Plan_Adj.Reriskcode%type,
                           v_riskcode Reins_Policy_Plan_Adj.Riskcode%type,
                           v_ttyid Reins_Policy_Plan_Adj.Ttyid%type,
                           v_endortimes Reins_Policy_Plan_Adj.Endortimes%type
                           ) is
  select *
    from Reins_Policy_Plan_Adj a
   where a.policyno = v_policyno
     and a.dangerunitno = v_dangerunitno
     and a.reriskcode = v_reriskcode
     and a.riskcode = v_riskcode
     and a.ttyid = v_ttyid
     and a.endortimes = v_endortimes;
 rec_PolicyPlanAdj cur_PolicyPlanAdj%rowtype;
 -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼�� end
begin

   for n_seq2 in 1..p_LastEdrAbs.count loop
      FindFlag:='N';
      for  n_seq1 in 1..p_NewEdrAbs.count loop
        if p_LastEdrAbs(n_seq2).TtyID=p_NewEdrAbs(n_seq1).TtyID then
          p_NewEdrAbs(n_seq1).GEarnedPrem :=nvl(p_LastEdrAbs(n_seq2).GEarnedPrem,0);
          p_NewEdrAbs(n_seq1).NEarnedPrem :=nvl(p_LastEdrAbs(n_seq2).NEarnedPrem,0);
          /*p_NewEdrAbs(n_seq1).GEarnedPrem :=0;
          p_NewEdrAbs(n_seq1).NEarnedPrem :=0;*/
          p_NewEdrAbs(n_seq1).ChgGrsPrem:=nvl(p_NewEdrAbs(n_seq1).GEarnedPrem,0)+nvl(p_NewEdrAbs(n_seq1).GPortfPrem,0)-nvl(p_LastEdrAbs(n_seq2).grsprem,0);
          p_NewEdrAbs(n_seq1).ChgNetPrem:=nvl(p_NewEdrAbs(n_seq1).NEarnedPrem,0)+nvl(p_NewEdrAbs(n_seq1).NPortfPrem,0)-nvl(p_LastEdrAbs(n_seq2).netprem,0);
          p_NewEdrAbs(n_seq1).GrsPrem:=nvl(p_NewEdrAbs(n_seq1).GEarnedPrem,0)+nvl(p_NewEdrAbs(n_seq1).GPortfPrem,0);
          p_NewEdrAbs(n_seq1).NetPrem:=nvl(p_NewEdrAbs(n_seq1).NEarnedPrem,0)+nvl(p_NewEdrAbs(n_seq1).NPortfPrem,0);
          p_NewEdrAbs(n_seq1).chgRiSum:=nvl(p_NewEdrAbs(n_seq1).RiSum,0)-nvl(p_LastEdrAbs(n_seq2).RiSum,0);
          p_NewEdrAbs(n_seq1).chgRIComm:=nvl(p_NewEdrAbs(n_seq1).ChgRIComm,0);
          if p_NewEdrAbs(n_seq1).netInd='0' then --ë����
             p_NewEdrAbs(n_seq1).ChgRIPrem:=p_NewEdrAbs(n_seq1).ChgGrsPrem;
             p_NewEdrAbs(n_seq1).RIPrem:=p_NewEdrAbs(n_seq1).GrsPrem;
          elsif p_NewEdrAbs(n_seq1).netInd='1' then --������
             p_NewEdrAbs(n_seq1).ChgRIPrem:=p_NewEdrAbs(n_seq1).ChgNetPrem;
             p_NewEdrAbs(n_seq1).RIPrem:=p_NewEdrAbs(n_seq1).NetPrem;
          end if;
          p_NewEdrAbs(n_seq1).ChgRiComm:=0;
    -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼�� begin
          v_Ricommrateadjind := 'N';
          for rec_ttySectReins in cur_ttySectReins(p_NewEdrAbs(n_seq1).TtyID,p_NewEdrAbs(n_seq1).sectno) loop
             v_Ricommrateadjind:=rec_ttySectReins.Ricommrateadjind;
             begin
                 open cur_PolicyPlanAdj(p_unit_para.policyno,
                                        p_unit_para.DangerUnitNo,
                                        p_unit_para.ReRiskCode,
                                        p_unit_para.RiskCode,
                                        p_NewEdrAbs(n_seq1).TtyID,
                                        p_unit_para.Endortimes);
                 fetch cur_PolicyPlanAdj
                  into rec_PolicyPlanAdj;
                 if cur_PolicyPlanAdj%notfound then
                     select '*' into v_char from dual where 1 = 2;
                 end if;
                 close cur_PolicyPlanAdj;
                 exception when others then
                    v_Ricommrateadjind := 'N';
               end;
             if rec_PolicyPlanAdj.Openind <> 'N' and v_Ricommrateadjind='Y' and rec_PolicyPlanAdj.Ricommrate is not null then
               p_NewEdrAbs(n_seq1).ChgRiComm:=p_NewEdrAbs(n_seq1).ChgRiComm+p_NewEdrAbs(n_seq1).ChgRiprem*rec_ttySectReins.Sharerate/100*rec_PolicyPlanAdj.Ricommrate/100;
             else
               p_NewEdrAbs(n_seq1).ChgRiComm:=p_NewEdrAbs(n_seq1).ChgRiComm+p_NewEdrAbs(n_seq1).ChgRiprem*rec_ttySectReins.Sharerate/100*rec_ttySectReins.Rcrate/100;
             end if;
       -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼�� end
             end loop;
          FindFlag:='Y';
        end if;
        if p_NewEdrAbs(n_seq1).taxind = '1' then
          p_NewEdrAbs(n_seq1).tax := p_NewEdrAbs(n_seq1).RIPrem * 0.06;
          p_NewEdrAbs(n_seq1).changetax := p_NewEdrAbs(n_seq1).ChgRIPrem * 0.06;
        else
          p_NewEdrAbs(n_seq1).tax := 0;
          p_NewEdrAbs(n_seq1).changetax :=0;
        end if;
      end loop;

      n_newcount:=p_NewEdrAbs.count;
      if FindFlag='N' then
        n_newcount:=n_newcount+1;
        p_NewEdrAbs(n_newcount).ttyid:=p_LastEdrAbs(n_seq2).ttyid;
        p_NewEdrAbs(n_newcount).ttycode:=p_LastEdrAbs(n_seq2).ttycode;
        p_NewEdrAbs(n_newcount).Uwyear:=p_LastEdrAbs(n_seq2).Uwyear;
        p_NewEdrAbs(n_newcount).sectno:=p_LastEdrAbs(n_seq2).sectno;
        p_NewEdrAbs(n_newcount).ttytype:=p_LastEdrAbs(n_seq2).ttytype;
        p_NewEdrAbs(n_newcount).statclass:=p_LastEdrAbs(n_seq2).statclass;
        p_NewEdrAbs(n_newcount).PCInd:=p_LastEdrAbs(n_seq2).PCInd;
        p_NewEdrAbs(n_newcount).NetInd:=p_LastEdrAbs(n_seq2).NetInd;
        p_NewEdrAbs(n_newcount).risum:=0;
        p_NewEdrAbs(n_newcount).ShareRate:=0;
        p_NewEdrAbs(n_newcount).RiCurr := p_LastEdrAbs(n_seq2).Ricurr;
        p_NewEdrAbs(n_newcount).ExchRate:=p_LastEdrAbs(n_seq2).ExchRate;
        p_NewEdrAbs(n_newcount).GPortfPrem:=p_LastEdrAbs(n_seq2).GPortfPrem;
        p_NewEdrAbs(n_newcount).NPortfPrem:=p_LastEdrAbs(n_seq2).NPortfPrem;
        /*p_NewEdrAbs(n_newcount).GPortfPrem:=0;
        p_NewEdrAbs(n_newcount).NPortfPrem:=0;*/
        p_NewEdrAbs(n_newcount).GEarnedPrem :=p_LastEdrAbs(n_seq2).GEarnedPrem;
        p_NewEdrAbs(n_newcount).NEarnedPrem :=p_LastEdrAbs(n_seq2).NEarnedPrem;
        /*p_NewEdrAbs(n_newcount).GEarnedPrem :=0;
        p_NewEdrAbs(n_newcount).NEarnedPrem :=0;*/
        p_NewEdrAbs(n_newcount).Chgrisum:=p_NewEdrAbs(n_newcount).risum-p_LastEdrAbs(n_seq2).RiSum;
        p_NewEdrAbs(n_newcount).ChgGrsPrem:=p_NewEdrAbs(n_newcount).GEarnedPrem-p_LastEdrAbs(n_seq2).grsprem;
        p_NewEdrAbs(n_newcount).ChgNetPrem:=p_NewEdrAbs(n_newcount).NEarnedPrem-p_LastEdrAbs(n_seq2).netprem;
        p_NewEdrAbs(n_newcount).GrsPrem:=p_NewEdrAbs(n_newcount).GEarnedPrem;
        p_NewEdrAbs(n_newcount).NetPrem:=p_NewEdrAbs(n_newcount).NEarnedPrem;
        p_NewEdrAbs(n_newcount).RIComm:=0;
        if p_NewEdrAbs(n_newcount).netInd='0' then --ë����
           p_NewEdrAbs(n_newcount).ChgRIPrem:=p_NewEdrAbs(n_newcount).ChgGrsPrem;
           p_NewEdrAbs(n_newcount).RIPrem:=p_NewEdrAbs(n_newcount).GrsPrem;
        elsif p_NewEdrAbs(n_newcount).netInd='1' then --������
           p_NewEdrAbs(n_newcount).ChgRIPrem:=p_NewEdrAbs(n_newcount).ChgNetPrem;
           p_NewEdrAbs(n_newcount).RIPrem:=p_NewEdrAbs(n_newcount).NetPrem;
        end if;
        p_NewEdrAbs(n_newcount).ChgRiComm:=0;
  -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼�� begin
        v_Ricommrateadjind := 'N';
        for rec_ttySectReins in cur_ttySectReins(p_NewEdrAbs(n_newcount).TtyID,p_NewEdrAbs(n_newcount).sectno) loop
             v_Ricommrateadjind:=rec_ttySectReins.Ricommrateadjind;
             begin
                 open cur_PolicyPlanAdj(p_unit_para.PolicyNo,
                                        p_unit_para.DangerUnitNo,
                                        p_unit_para.ReRiskCode,
                                        p_unit_para.RiskCode,
                                        p_NewEdrAbs(n_newcount).TtyID,
                                        p_unit_para.Endortimes);
                 fetch cur_PolicyPlanAdj
                  into rec_PolicyPlanAdj;
                 if cur_PolicyPlanAdj%notfound then
                     select '*' into v_char from dual where 1 = 2;
                 end if;
                 close cur_PolicyPlanAdj;
                 exception when others then
                    v_Ricommrateadjind := 'N';
               end;
             if rec_PolicyPlanAdj.Openind <> 'N' and v_Ricommrateadjind='Y' and rec_PolicyPlanAdj.Ricommrate is not null then
                p_NewEdrAbs(n_newcount).ChgRiComm:=p_NewEdrAbs(n_newcount).ChgRiComm+p_NewEdrAbs(n_newcount).ChgRiprem*rec_ttySectReins.Sharerate/100*rec_PolicyPlanAdj.Ricommrate/100;
             else
                p_NewEdrAbs(n_newcount).ChgRiComm:=p_NewEdrAbs(n_newcount).ChgRiComm+p_NewEdrAbs(n_newcount).ChgRiprem*rec_ttySectReins.Sharerate/100*rec_ttySectReins.Rcrate/100;
             end if;
        end loop;
  -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼�� end
        if p_NewEdrAbs(n_newcount).taxind = '1' then
          p_NewEdrAbs(n_newcount).tax := p_NewEdrAbs(n_newcount).RIPrem * 0.06;
          p_NewEdrAbs(n_newcount).changetax := p_NewEdrAbs(n_newcount).ChgRIPrem * 0.06;
        else
          p_NewEdrAbs(n_newcount).tax := 0;
          p_NewEdrAbs(n_newcount).changetax := 0;
        end if;
      end if;
   end loop;

   for n_seq2 in 1..p_NewEdrAbs.count loop
      FindFlag:='N';
      for n_seq1 in 1..p_LastEdrAbs.count loop
        if p_LastEdrAbs(n_seq1).TtyID=p_NewEdrAbs(n_seq2).TtyID then
           FindFlag:='Y';
           exit;
        end if;
      end loop;
      if(FindFlag='N') then
         p_NewEdrAbs(n_seq2).chgRiSum:= p_NewEdrAbs(n_seq2).RiSum;
      end if;
   end loop;

end jion_re_rslt;

/*̯�ؼ���ʱ����ȡ�ð��ֱ����ķֳ������������ʱ�ֳ��ĺ�Լ�ѽ�����ֳ�������*/
procedure get_p_share(p_reclm_para in reclm_para,
                      p_share  out Arr_danger_share) is
  cursor cur_p_reshare is
     select a.TtyID,a.ttycode,a.uwyear,a.sectno,a.ttytype,a.statclass,a.PCInd,
            a.ShareRate,a.RICurr, a.RISum,to_char(b.reriskcode),b.dangertype
     from REINS_REPLY_SHARE a,REINS_REPOLICY b
    where a.RepolicyNo=b.RepolicyNo
      and a.RecTimes=b.RecTimes
      and b.Status='1'
      and b.policyNo=p_reclm_para.policyno
      and b.DANGERUNITNO=p_reclm_para.DANGERUNITNO
      and b.riskcode=p_reclm_para.riskcode
      and b.reriskcode=p_reclm_para.reriskcode
      and b.restartdate<=p_reclm_para.DamageDATE
      and b.reenddate>=p_reclm_para.DamageDATE
   --   and (a.pcind='P' or statclass='1')
  union all
    select a.TtyID,a.ttyCode,a.uwyear,a.sectno,a.ttytype,a.statclass,a.PCInd,
           a.ShareRate,a.RICurr,a.RISum,b.reriskcode,b.dangertype
     from REINS_REENDR_SHARE a,REINS_REENDOR b
    where a.RepolicyNo=b.RepolicyNo
      and a.reendortimes=b.reendortimes
      and a.RecTimes=b.RecTimes
      and b.Status='1'
      and b.policyNo=p_reclm_para.policyno
      and b.DANGERUNITNO=p_reclm_para.DANGERUNITNO
      and b.riskcode=p_reclm_para.riskcode
      and b.reriskcode=p_reclm_para.reriskcode
      and b.restartdate<=p_reclm_para.DamageDATE
      and b.reenddate>=p_reclm_para.DamageDATE;
   --   and (a.pcind='P' or statclass='1');
   rec_P_reshare cur_P_reshare%rowtype;

  n_seq number(3);
  n_seq1 number(3);
  n_seq2 number(3);
  n_newcnt number(3);
  FindFlag varchar2(1);
  v_share Arr_danger_share;
  v_nfac_ttycode REINS_TTY_TABLE.ttycode%type;  --�ٷֳ����Լ
  v_reten_ttycode REINS_TTY_TABLE.ttycode%type; --����������Լ
 begin
    n_seq:=1;
    select ttycode into v_nfac_ttycode from REINS_TTY_TABLE where ttytype='92';
    select ttycode into v_reten_ttycode from REINS_TTY_TABLE where ttytype='82';
    for  rec_p_reshare  in cur_p_reshare loop
      if rec_p_reshare.dangertype  not in ('A','B','C')  then
         v_share(n_seq).ttyid:=rec_p_reshare.ttyid;
         v_share(n_seq).SectNo:=rec_p_reshare.sectno;
         v_share(n_seq).statclass:=rec_p_reshare.statclass;
         v_share(n_seq).PCInd:='P';
         v_share(n_seq).sharerate:=rec_p_reshare.sharerate;
         --��̯ʱ���ٷֳ���ֳ��ݶ�ת��Ϊ��������
         if substr(v_share(n_seq).ttyid,1,5)=v_nfac_ttycode then
           v_share(n_seq).ttyid:=v_reten_ttycode||substr(v_share(n_seq).ttyid,6);
           v_share(n_seq).statclass:='1';
         end if;
         --�ű���ԼSMEQS2015�ѳ����⸶�ʣ�������̯�� ת��Ϊ��������
      --��������������Լ�����������Ӧ�⸶���ֶΣ������ֶ�ֵ���ж�
      --��������ʱд����ԼID
      --����ǰ���Ƴ���ԤԼ��ͬ��MQHOP�����巽��������  HA20221109610
      if rec_P_reshare.ttyid in('SMEQS2015','MQHOP2019','MQHOP2018','MQHOP2017') then
        v_share(n_seq).ttyid:=v_reten_ttycode||substr(v_share(n_seq).ttyid,6);
        v_share(n_seq).statclass:='1';
      end if;
      --�¼� COMPU��ԼʧЧ�ܿ�  add by oycy 20220826
       if substr(rec_P_reshare.ttyid,1,5) in( 'COMPU','WA1QS') then
        v_share(n_seq).ttyid:=v_reten_ttycode||substr(v_share(n_seq).ttyid,6);
        v_share(n_seq).statclass:='1';
      end if;
         n_seq:=n_seq+1;
      elsif rec_p_reshare.dangertype in ('A','B','C') /* and rec_p_reshare.PCIND='P'*/ then
         v_share(n_seq).ttyid:=rec_p_reshare.ttyid;
         v_share(n_seq).SectNo:=rec_p_reshare.sectno;
         v_share(n_seq).statclass:=rec_p_reshare.statclass;
         v_share(n_seq).PCInd:='P';
         v_share(n_seq).sharerate:=rec_p_reshare.sharerate;
         --��̯ʱ���ٷֳ���ֳ��ݶ�ת��Ϊ��������
         if substr(v_share(n_seq).ttyid,1,5)=v_nfac_ttycode then
           v_share(n_seq).ttyid:=v_reten_ttycode||substr(v_share(n_seq).ttyid,6);
           v_share(n_seq).statclass:='1';
         end if;

         --�ű���ԼSMEQS2015�ѳ����⸶�ʣ�������̯�� ת��Ϊ��������
      --��������������Լ�����������Ӧ�⸶���ֶΣ������ֶ�ֵ���ж�
      --��������ʱд����ԼID
      --����ǰ���Ƴ���ԤԼ��ͬ��MQHOP�����巽��������  HA20221109610
      if rec_P_reshare.ttyid in('SMEQS2015','MQHOP2019','MQHOP2018','MQHOP2017')  then
        v_share(n_seq).ttyid:=v_reten_ttycode||substr(v_share(n_seq).ttyid,6);
        v_share(n_seq).statclass:='1';
      end if;
      --�¼� COMPU��ԼʧЧ�ܿ�  add by oycy 20220826
       if substr(rec_P_reshare.ttyid,1,5) in( 'COMPU','WA1QS') then
        v_share(n_seq).ttyid:=v_reten_ttycode||substr(v_share(n_seq).ttyid,6);
        v_share(n_seq).statclass:='1';
      end if;
         n_seq:=n_seq+1;
      end if;

      --n_seq:=n_seq+1;
    end loop;

    --�ϲ�ͬһ��ԼID(����ĸ�������)
    n_seq1:=1;
    for n_seq1 in 1..v_share.count loop
      n_newcnt:=p_share.count;
      FindFlag:='N';
      n_seq2:=1;
      for  n_seq2 in 1..p_share.count loop
        if v_share(n_seq1).TtyID=p_share(n_seq2).TtyID then
          p_share(n_seq2).sharerate:=p_share(n_seq2).sharerate+v_share(n_seq1).sharerate;
          FindFlag:='Y';
        end if;
      end loop;

      if FindFlag='N' then
        n_newcnt:=n_newcnt+1;
        p_share(n_newcnt).ttyid:=v_share(n_seq1).ttyid;
        p_share(n_newcnt).sectno:=v_share(n_seq1).sectno;
        p_share(n_newcnt).statclass:=v_share(n_seq1).statclass;
        p_share(n_newcnt).pcind:=v_share(n_seq1).pcind;
        p_share(n_newcnt).sharerate:=v_share(n_seq1).sharerate;
      end if;
    end loop;
 end get_p_share;

/*̯�ؼ���ʱ����ȡ�ð�contribution �ķֳ������������ʱ�ֳ��ĺ�Լ�ѽ�����ֳ�������
����ʱҪ���ֳ�����ȫ���۳�ԭʼ�ⰸ���ֽ��м���*/
 procedure get_c_share(p_reclm_para in reclm_para,
                       p_share out  Arr_danger_share) is

 cursor cur_Danger_share is
  select a.TtyID,a.ttycode,a.statclass,a.sectno,a.oriCurr,a.RISum/exchrate risum,to_char(b.reriskcode)
     from REINS_REPLY_SHARE a,REINS_REPOLICY b
    where b.DangerCode=p_reclm_para.DangerCode
      and b.restartdate<=p_reclm_para.DamageDATE
      and b.reenddate>=p_reclm_para.DamageDATE
      and b.Status='1'
      and a.pcind='C'
      and a.dangertype=p_reclm_para.Dangertype
      and a.RepolicyNo=b.RepolicyNo
      and a.RecTimes=b.RecTimes
  union all
    select a.TtyID,a.ttycode,a.statclass,a.sectno,a.oriCurr,a.RISum/exchrate risum,b.reriskcode
     from REINS_REENDR_SHARE a,REINS_REENDOR b
    where b.DangerCode=p_reclm_para.DangerCode
      and b.restartdate<=p_reclm_para.DamageDATE
      and b.reenddate>=p_reclm_para.DamageDATE
      and b.Status='1'
      and a.pcind='C'
      and a.RepolicyNo=b.RepolicyNo
      and a.reendortimes=b.reendortimes
      and a.dangertype=p_reclm_para.Dangertype
      and a.RecTimes=b.RecTimes;

 rec_Danger_share cur_Danger_share%rowtype;
 v_share arr_danger_share;

 n_seq number(3);
 n_seq1 number(3);
 n_seq2 number(3);
 n_newcnt number(3);
 FindFlag varchar2(1);
 v_tot_pml REINS_REPOLICY.PML%type;
 v_nfac_ttycode REINS_TTY_TABLE.ttycode%type;  --�ٷֳ����Լ
 v_reten_ttycode REINS_TTY_TABLE.ttycode%type; --����������Լ
-- v_new_ttyid   REINS_TREATY.ttyid%type;
 --v_new_sectno  REINS_TTY_SECT.sectno%type;
begin
  select ttycode into v_nfac_ttycode from REINS_TTY_TABLE where ttytype='92';
  select ttycode into v_reten_ttycode from REINS_TTY_TABLE where ttytype='82';
  v_tot_pml:=get_danger_c_PML(p_reclm_para.DangerCode,p_reclm_para.currency,p_reclm_para.DamageDATE,p_reclm_para.Dangertype);
  if v_tot_pml<>0 then
    n_seq:=1;
    for rec_danger_share in cur_danger_share loop
       -- v_share(n_seq).TtyID :=v_new_ttyid;
       --v_share(n_seq).sectno:=v_new_sectno;
       v_share(n_seq).TtyID :=rec_danger_share.TtyID;
       v_share(n_seq).sectno:=rec_danger_share.sectNo;
       v_share(n_seq).statclass:=rec_danger_share.statclass;
       v_share(n_seq).PCInd:='C';
       v_share(n_seq).riCurr:=rec_danger_share.ORICurr;
       v_share(n_seq).RISum:=rec_danger_share.RISum;
       --��̯ʱ���ٷֳ���ֳ��ݶ�ת��Ϊ��������
       if substr(v_share(n_seq).ttyid,1,5)=v_nfac_ttycode then
          v_share(n_seq).ttyid:=v_reten_ttycode||substr(v_share(n_seq).ttyid,6);
          v_share(n_seq).statclass:='1';
       end if;
       n_seq:=n_seq+1;
    end loop;

    --��ʼ������ͬ��Լ����
    n_seq1:=1;
    for n_seq1 in 1..v_share.count loop
      n_newcnt:=p_share.count;
      FindFlag:='N';
      n_seq2:=1;
      for  n_seq2 in 1..p_share.count loop
       if v_share(n_seq1).TtyID=p_share(n_seq2).TtyID then
        p_share(n_seq2).RISum:=p_share(n_seq2).RISum + v_share(n_seq1).RISum*get_exchrate(v_share(n_seq1).ricurr,p_reclm_para.currency,p_reclm_para.DamageDATE);
        p_share(n_seq2).sharerate:=p_share(n_seq2).RISum*100/v_tot_pml;
        FindFlag:='Y';
       end if;
      end loop;

      if FindFlag='N' then
       n_newcnt:=n_newcnt+1;
       p_share(n_newcnt).ricurr:=p_reclm_para.currency;
       p_share(n_newcnt).ttyid:=v_share(n_seq1).ttyid;
       p_share(n_newcnt).sectno:=v_share(n_seq1).sectno;
       p_share(n_newcnt).statclass:=v_share(n_seq1).statclass;
       p_share(n_newcnt).pcind:=v_share(n_seq1).pcind;
       p_share(n_newcnt).risum:=v_share(n_seq1).risum * get_exchrate(v_share(n_seq1).ricurr,p_reclm_para.currency,p_reclm_para.DamageDATE);
       p_share(n_newcnt).sharerate:=p_share(n_newcnt).RISum*100/v_tot_pml;
      end if;
    end loop;

  end if;

end get_c_share;

--�����ٷֵİ�contribution��̯�ؽ��������ͬget_ply_c_share
procedure get_fac_c_share(p_reclm_para in reclm_para,
                          p_share  out Arr_danger_fac_share) is
 cursor cur_Danger_share is
  select a.confertype,a.brokercode,a.reinscode,a.RICurr,a.RISum,a.PCInd,
         a.PAYCODE,a.AGENTCODE,a.CONFERNO,a.RIREFNO
     from REINS_REPLY_FAC a,REINS_REPOLICY b
    where b.DangerCode=p_reclm_para.DangerCode
      and b.restartdate<=p_reclm_para.DamageDATE
      and b.reenddate>=p_reclm_para.DamageDATE
      and b.Status='1'
      and b.reinstype='1'
      and a.pcind='C'
      and a.RepolicyNo=b.RepolicyNo
      and a.RecTimes=b.RecTimes
  union all
    select a.confertype,a.brokercode,a.reinscode,a.RICurr,a.RISum,A.PCInd,
           a.PAYCODE,a.AGENTCODE,a.CONFERNO,a.RIREFNO
     from REINS_REENDR_FAC a,REINS_REENDOR b
    where b.DangerCode=p_reclm_para.DangerCode
      and b.restartdate<=p_reclm_para.DamageDATE
      and b.reenddate>=p_reclm_para.DamageDATE
      and b.Status='1'
      and b.reinstype='1'
      and a.pcind='C'
      and a.RepolicyNo=b.RepolicyNo
      and a.RecTimes=b.RecTimes
      and a.reendortimes=b.reendortimes;

 rec_Danger_share cur_Danger_share%rowtype;

 n_seq number(3);
 v_share arr_danger_fac_share;
 n_seq1 number(3);
 n_seq2 number(3);
 n_newcnt number(3);
 FindFlag varchar2(1);
 v_tot_pml REINS_REPOLICY.PML%type;
 begin
  v_tot_pml:=get_danger_c_PML(p_reclm_para.DangerCode,p_reclm_para.currency,p_reclm_para.DamageDATE,p_reclm_para.dangertype);
  if v_tot_pml<>0 then
  n_seq:=1;
  for rec_danger_share in cur_danger_share loop
    v_share(n_seq).confertype :=rec_danger_share.confertype;
    v_share(n_seq).brokercode:=rec_danger_share.brokercode;
    v_share(n_seq).reinscode:=rec_danger_share.reinscode;
    v_share(n_seq).PAYCODE:=rec_danger_share.PAYCODE;
    v_share(n_seq).AGENTCODE:=rec_danger_share.AGENTCODE;
    v_share(n_seq).CONFERNO:=rec_danger_share.CONFERNO;
    v_share(n_seq).RIREFNO:=rec_danger_share.RIREFNO;
    v_share(n_seq).RICurr:=rec_danger_share.RICurr;
    v_share(n_seq).RISum:=rec_danger_share.RISum;
    v_share(n_seq).PCInd:=rec_danger_share.PCInd;
    n_seq:=n_seq+1;
  end loop;

  n_seq1:=1;
  n_seq2:=1;
      for n_seq1 in 1..v_share.count loop
      n_newcnt:=p_share.count;
      FindFlag:='N';
      for  n_seq2 in 1..p_share.count loop
       if v_share(n_seq1).confertype=p_share(n_seq2).confertype and
          nvl(v_share(n_seq1).brokercode,'*')=nvl(p_share(n_seq2).brokercode,'*') and
          v_share(n_seq1).reinscode=p_share(n_seq2).reinscode then
          p_share(n_seq2).RISum:=p_share(n_seq2).RISum + v_share(n_seq1).RISum * get_exchrate(v_share(n_seq1).ricurr,p_reclm_para.currency,p_reclm_para.DamageDATE);
          p_share(n_seq2).sharerate:=p_share(n_seq2).RISum*100/v_tot_pml;
          FindFlag:='Y';
       end if;
      end loop;
      if FindFlag='N' then
       n_newcnt:=n_newcnt+1;
       p_share(n_newcnt).ricurr:=p_reclm_para.currency;
       p_share(n_newcnt).confertype:=v_share(n_seq1).confertype;
       p_share(n_newcnt).brokercode:=v_share(n_seq1).brokercode;
       p_share(n_newcnt).reinscode:=v_share(n_seq1).reinscode;
       p_share(n_newcnt).PAYCODE:=v_share(n_seq1).PAYCODE;
       p_share(n_newcnt).AGENTCODE:=v_share(n_seq1).AGENTCODE;
       p_share(n_newcnt).CONFERNO:=v_share(n_seq1).CONFERNO;
       p_share(n_newcnt).RIREFNO:=v_share(n_seq1).RIREFNO;
       p_share(n_newcnt).PCInd:=v_share(n_seq1).PCInd;
       p_share(n_newcnt).risum:=v_share(n_seq1).risum * get_exchrate(v_share(n_seq1).ricurr,p_reclm_para.currency,p_reclm_para.DamageDATE);
       p_share(n_newcnt).sharerate:=p_share(n_newcnt).RISum*100/v_tot_pml;
      end if;
      end loop;
   end if;
  end get_fac_c_share;

--ȡ�ٷֵİ��ֱ���̯�ؽ��������ͬget_ply_p_share
procedure get_fac_p_share(p_reclm_para in reclm_para,
                          p_share out Arr_danger_fac_share) is
 cursor cur_Danger_share is

  select a.confertype,a.brokercode,a.reinscode,a.RICurr,a.RISum,a.PCInd,a.sharerate,
         a.PAYCODE,a.AGENTCODE,a.CONFERNO,a.RIREFNO,
         a.insurancetype,a.interestinsured,a.remarks,a.conditions,a.deductibles
     from REINS_REPLY_FAC a,REINS_REPOLICY b
    where b.policyNo=p_reclm_para.policyno
      and b.DANGERUNITNO=p_reclm_para.DANGERUNITNO
      and b.riskcode=p_reclm_para.riskcode
      and b.reriskcode=p_reclm_para.reriskcode
      and b.restartdate<=p_reclm_para.DamageDATE
      and b.reenddate>=p_reclm_para.DamageDATE
      and b.Status='1'
      and b.reinstype='1'
      and a.pcind='P'
      and a.RepolicyNo=b.RepolicyNo
      and a.RecTimes=b.RecTimes
  union all
    select a.confertype,a.brokercode,a.reinscode,a.RICurr,a.RISum,A.PCInd,a.sharerate,
           a.PAYCODE,a.AGENTCODE,a.CONFERNO,a.RIREFNO,
           a.insurancetype,a.interestinsured,a.remarks,a.conditions,a.deductibles
     from REINS_REENDR_FAC a,REINS_REENDOR b
    where b.policyNo=p_reclm_para.policyno
      and b.DANGERUNITNO=p_reclm_para.DANGERUNITNO
      and b.riskcode=p_reclm_para.riskcode
      and b.reriskcode=p_reclm_para.reriskcode
      and b.restartdate<=p_reclm_para.DamageDATE
      and b.reenddate>=p_reclm_para.DamageDATE
      and b.Status='1'
      and b.reinstype='1'
      and a.pcind='P'
      and a.RepolicyNo=b.RepolicyNo
      and a.RecTimes=b.RecTimes
      and a.reendortimes=b.reendortimes;

 rec_Danger_share cur_Danger_share%rowtype;

 n_seq number(3);
 begin
  n_seq:=1;
  for rec_danger_share in cur_danger_share loop
    p_share(n_seq).confertype :=rec_danger_share.confertype;
    p_share(n_seq).brokercode:=rec_danger_share.brokercode;
    p_share(n_seq).reinscode:=rec_danger_share.reinscode;
    p_share(n_seq).PAYCODE:=rec_danger_share.PAYCODE;
    p_share(n_seq).AGENTCODE:=rec_danger_share.AGENTCODE;
    p_share(n_seq).CONFERNO:=rec_danger_share.CONFERNO;
    p_share(n_seq).RIREFNO:=rec_danger_share.RIREFNO;
    p_share(n_seq).RICurr:=rec_danger_share.RICurr;
    p_share(n_seq).RISum:=rec_danger_share.RISum;
    p_share(n_seq).sharerate:=rec_danger_share.sharerate;
    p_share(n_seq).PCInd:=rec_danger_share.PCInd;
    p_share(n_seq).insurancetype := rec_danger_share.insurancetype;
    p_share(n_seq).interestinsured := rec_danger_share.interestinsured;
    p_share(n_seq).remarks := rec_danger_share.remarks;
    p_share(n_seq).conditions := rec_danger_share.conditions;
    p_share(n_seq).deductibles := rec_danger_share.deductibles;
    n_seq:=n_seq+1;
  end loop;

end get_fac_p_share;

--��Լ�ֱ����ĸ��ˣ�����ʱ�������ڸ��������ֱַ����
procedure repolicy_check(p_repolicyno in REINS_REPOLICY.repolicyno%type,
                         p_rectimes   in REINS_REPOLICY.rectimes%type
                         ) is
v_oriGrsprem REINS_REPOLICY.Pml%type;
 cursor cur_ply is
     select * from REINS_REPOLICY
      where repolicyno=p_repolicyno
        and rectimes=p_rectimes
        and checkind='0' for update;
 rec_ply cur_ply%rowtype;

 v_plandate date;
 cursor cur_plan(v_INSTMARK REINS_REPOLICY.INSTMARK%type )is
     select 1 PayNo,v_plandate PlanDate,1 planrate
       from dual
      where v_INSTMARK='N'
    union
     select payno,PlanDate,GRSPLANFEE/v_oriGrsprem  planrate
       from REINS_POLICY_UNIT_PLAN
      where v_INSTMARK='Y'
        and PolicyNo=rec_ply.PolicyNo
        and DangerUnitNo=rec_ply.DangerUnitNo
        and ReRiskCode=rec_ply.ReRiskCode
        and RiskCode=rec_ply.RiskCode;
 rec_plan cur_plan%rowtype;

 cursor cur_ply_share is
      select * from REINS_REPLY_SHARE
       where repolicyno=p_repolicyno
         and rectimes=p_rectimes;
 rec_ply_share cur_ply_share%rowtype;

 v_paydate date;
 v_char char(1);
 v_sum_premium REINS_RE_PLAN.premium%type:=0;
begin
  open cur_ply;
   fetch cur_ply into rec_ply;
   if cur_ply%notfound then
    close cur_ply;
    g_errmsg :='�ֱ���������';
    select '*' into v_char from dual where 1 = 2;
   end if;
  close cur_ply;

  g_errmsg :='��ȡΣ�յ�λë����';
  select GrsPrem into v_oriGrsprem
    from REINS_POLICY_UNIT
   where PolicyNo=rec_ply.PolicyNo
     and DangerUnitNo=rec_ply.DangerUnitNo
     and ReRiskCode=rec_ply.ReRiskCode
     and RiskCode=rec_ply.RiskCode;

  v_plandate := greatest(rec_ply.restartdate,rec_ply.uwenddate);
  v_paydate := GRBillDeal.get_bill_date(v_plandate);

   open cur_plan(rec_ply.INSTMARK);
   fetch cur_plan into rec_plan;
     if cur_plan%notfound then
       g_errmsg:='����û�з�����Ϣ';
       close cur_plan;
       select '*' into v_char from dual where 1=2;
     end if;
   close cur_plan;

   open cur_ply_share;
   fetch cur_ply_share into rec_ply_share;
     if cur_ply_share%notfound then
       g_errmsg:='����û�зֳ����';
       close cur_ply_share;
       select '*' into v_char from dual where 1=2;
     end if;
   close cur_ply_share;

  for rec_ply_share in cur_ply_share loop
    for rec_plan in cur_plan(rec_ply.INSTMARK) loop
      insert into REINS_RE_PLAN(
         REPOLICYNO       ,
         RECTIMES         ,
         REENDORTIMES   ,
         TTYID            ,
         PAYTIMES         ,
         TTYTYPE          ,
         POLICYNO         ,
         DANGERUNITNO     ,
         COMCODE          ,
         RERISKCODE       ,
         RISKCODE         ,
         UWYEAR           ,
         SECTNO           ,
         CURRENCY         ,
         AMOUNT           ,
         PREMIUM          ,
         COMMISSION       ,
         PAYDATE          ,
         startdate        ,
         ENDDATE          ,
         REstartdate      ,
         REENDDATE        ,
         ACCIND          ,
         Businessind,
         Endortimes,
         INSTIND,
         ACCYM,
         ACCCHANNEL,
         PlanCode,
         netind,
         sharerate,
         exchrate,
         oricurr,
         instamt,
         Instgrsprem,
         instnetprem,
         date_created,
         date_updated,
         tax,--modify by wangmx 20170708
         taxInd)
      select
         rec_ply_share.REPOLICYNO     ,
         rec_ply_share.RECTIMES       ,
         '000' ,
         rec_ply_share.TTYID          ,
         rec_plan.PAYNo               ,
         rec_ply_share.TTYTYPE        ,
         rec_ply_share.POLICYNO       ,
         rec_ply_share.DANGERUNITNO   ,
         rec_ply_share.COMCODE        ,
         rec_ply_share.RERISKCODE     ,
         rec_ply_share.RISKCODE       ,
         rec_ply_share.UWYEAR         ,
         rec_ply_share.SECTNO         ,
         rec_ply_share.Ricurr         ,
         decode(rec_plan.PAYNo,1,rec_ply_share.RISum,0)  ,
         rec_ply_share.RIPREM *rec_plan.planrate      ,
         rec_ply_share.RICOMM *rec_plan.planrate      ,
         GRBillDeal.get_bill_date(rec_plan.plandate)             ,
         rec_ply_share.startdate      ,
         rec_ply_share.ENDDATE        ,
         rec_ply_share.REstartdate    ,
         rec_ply_share.REENDDATE      ,
         'N',                    -- �����ʵ��� Y/N
         rec_ply_share.businessInd,
         '000',
         rec_ply.INSTMARK,
         to_char(v_paydate ,'YYYYMM'),
         rec_ply_share.ACCCHANNEL,
         rec_ply_share.PlanCode,
         rec_ply_share.netind,
         rec_ply_share.sharerate,
         rec_ply_share.exchrate,
         rec_ply.currency,
         rec_ply.pml,
         rec_ply.grsprem*rec_plan.planrate,
         rec_ply.netprem*rec_plan.planrate,
         sysdate,
         sysdate,
         rec_ply_share.tax * rec_plan.planrate,--modify by wangmx 20170708
         rec_ply.taxind
      from dual;
    end loop;

    --����Լ�Է��ڱ��ѽ��е�������ֵ����1�����쳣
    if rec_ply.INSTMARK='Y' then
      select sum(premium)
        into v_sum_premium
        from REINS_RE_PLAN
       where repolicyno = rec_ply_share.repolicyno
         and reendortimes = '000'
         and rectimes = rec_ply_share.rectimes
         and ttyid = rec_ply_share.ttyid;
      if abs(rec_ply_share.riprem-v_sum_premium)>=1 then
        g_errmsg:='��Լ���ڱ���֮����ֱ����������1';
        select '*' into v_char from dual where 1=2;
      elsif abs(rec_ply_share.riprem-v_sum_premium)>=0.01 then
        update REINS_RE_PLAN
           set premium = premium + (rec_ply_share.riprem - v_sum_premium)
         where repolicyno = rec_ply_share.repolicyno
           and reendortimes = '000'
           and rectimes = rec_ply_share.rectimes
           and ttyid = rec_ply_share.ttyid
           and paytimes=1;
      end if;
    end if;
  end loop;

  update REINS_REPOLICY set checkind='1',checkdate=v_paydate,date_updated = sysdate
   where repolicyno=p_repolicyno and rectimes=p_rectimes;

   exception when others then
  --���������־
   g_errmsg:=g_errmsg||'-'||substr(sqlerrm,1,100);
   rollback;
   insert into REINS_POLICY_ERR_LOG(errtype,repolicyno,rectimes,errmsg)
         values('C1',p_repolicyno,p_rectimes,g_errmsg);

end repolicy_check;

--��Լ�������ĸ��ˣ�����ʱ�������ڸ��������ֱַ����
procedure reendor_check(p_repolicyno in REINS_REPOLICY.repolicyno%type,
                        p_reendortimes in REINS_REPOLICY.reendortimes%type,
                        p_rectimes   in REINS_REPOLICY.rectimes%type
                        ) is
 cursor cur_ply is
     select * from REINS_REENDOR
      where repolicyno=p_repolicyno
        and reendortimes=p_reendortimes
        and rectimes=p_rectimes
        and checkind='0' for update;
 rec_ply cur_ply%rowtype;

 v_plandate date;
 cursor cur_plan(v_INSTMARK REINS_REPOLICY.INSTMARK%type,v_chgGrsprem REINS_REPOLICY.Pml%type, v_endortimes REINS_REPOLICY.Endortimes%type)is
     select 1 PayNo,v_plandate PlanDate,1 planrate
       from dual
      where v_INSTMARK='N'
    union
     select payno,PlanDate,decode(v_chgGrsprem,0,1,ChgGRSPLANFEE/v_chgGrsprem) planrate
       from REINS_ENDOR_UNIT_PLAN
      where v_INSTMARK='Y'
        and PolicyNo=rec_ply.PolicyNo
        and EndorTimes=rec_ply.EndorTimes
        and DangerUnitNo=rec_ply.DangerUnitNo
        and ReRiskCode=rec_ply.ReRiskCode
        and RiskCode=rec_ply.RiskCode
        and v_endortimes<>'000'
     union
     select payno,PlanDate,decode(v_chgGrsprem,0,1,GRSPLANFEE/v_chgGrsprem)  planrate
       from REINS_POLICY_UNIT_PLAN
      where v_INSTMARK='Y'
        and PolicyNo=rec_ply.PolicyNo
        and DangerUnitNo=rec_ply.DangerUnitNo
        and ReRiskCode=rec_ply.ReRiskCode
        and RiskCode=rec_ply.RiskCode
        and v_endortimes='000';
 rec_plan cur_plan%rowtype;

 cursor cur_ply_share is
      select * from REINS_REENDR_SHARE
       where repolicyno=p_repolicyno
         and reendortimes=p_reendortimes
         and rectimes=p_rectimes;
 rec_ply_share cur_ply_share%rowtype;

 v_endortimes REINS_REENDOR.endortimes%type;
 v_chgGrsprem REINS_REENDOR.Pml%type;
 v_paydate date;
 v_char char(1);
 v_sum_premium REINS_RE_PLAN.premium%type:=0;
 begin
  open cur_ply;
   fetch cur_ply into rec_ply;
   if cur_ply%notfound then
    close cur_ply;
    g_errmsg := '������������';
    select '*' into v_char from dual where 1 = 2;
   end if;
  close cur_ply;
  if(rec_ply.ChgGrsPrem=0) then
     rec_ply.ChgGrsPrem:=1;
  end if;

  v_endortimes := lpad(nvl(rec_ply.endortimes,'000'),3,'0');
  /*select lpad(nvl(endortimes,'000'),3,'0') into v_endortimes
        from REINS_REENDOR where repolicyno=p_repolicyno and rectimes=p_rectimes and reendortimes=p_reendortimes;*/
  g_errmsg := '��ȡΣ�յ�λë����';
  if v_endortimes<>'000' then
   select chggrsprem into v_chgGrsprem
     from REINS_ENDOR_UNIT
    where PolicyNo=rec_ply.PolicyNo
      and EndorTimes=rec_ply.EndorTimes
      and DangerUnitNo=rec_ply.DangerUnitNo
      and ReRiskCode=rec_ply.ReRiskCode
      and RiskCode=rec_ply.RiskCode;
  else
   select grsprem into v_chgGrsprem
     from REINS_POLICY_UNIT
    where PolicyNo=rec_ply.PolicyNo
      and DangerUnitNo=rec_ply.DangerUnitNo
      and ReRiskCode=rec_ply.ReRiskCode
      and RiskCode=rec_ply.RiskCode;
  end if;

  v_plandate := greatest(rec_ply.restartdate,rec_ply.uwenddate);
  v_paydate := GRBillDeal.get_bill_date(v_plandate);

   open cur_plan(rec_ply.INSTMARK,v_chgGrsprem,v_endortimes);
   fetch cur_plan into rec_plan;
     if cur_plan%notfound then
       g_errmsg:='����û�з�����Ϣ';
       close cur_plan;
       select '*' into v_char from dual where 1=2;
     end if;
   close cur_plan;

   open cur_ply_share;
   fetch cur_ply_share into rec_ply_share;
     if cur_ply_share%notfound then
       g_errmsg:='������û�зֳ����';
       close cur_ply_share;
       select '*' into v_char from dual where 1=2;
     end if;
   close cur_ply_share;

  for rec_ply_share in cur_ply_share loop
    for rec_plan in cur_plan(rec_ply.INSTMARK,v_chgGrsprem,v_endortimes) loop
      insert into REINS_RE_PLAN(
         REPOLICYNO       ,
         RECTIMES         ,
         REendortimeS   ,
         TTYID            ,
         PAYTIMES         ,
         TTYTYPE          ,
         POLICYNO         ,
         DANGERUNITNO     ,
         COMCODE          ,
         RERISKCODE       ,
         RISKCODE         ,
         UWYEAR           ,
         SECTNO           ,
         CURRENCY         ,
         AMOUNT           ,
         PREMIUM          ,
         COMMISSION       ,
         PAYDATE          ,
         startdate        ,
         ENDDATE          ,
         REstartdate      ,
         REENDDATE        ,
         ACCIND           ,
         businessInd,
         Endortimes,
         INSTIND,
         ACCYM,
         ACCCHANNEL,
         PlanCode,
         netind,
         sharerate,
         exchrate,
         oricurr,
         instamt,
         Instgrsprem,
         instnetprem,
         date_created,
         date_updated,
         tax,--modify by wangmx 20170708
         taxInd)
      select
         rec_ply_share.REPOLICYNO     ,
         rec_ply_share.RECTIMES       ,
         rec_ply_share.reendortimes ,
         rec_ply_share.TTYID           ,
         rec_plan.PAYNo               ,
         rec_ply_share.TTYTYPE        ,
         rec_ply_share.POLICYNO       ,
         rec_ply_share.DANGERUNITNO   ,
         rec_ply_share.COMCODE        ,
         rec_ply_share.RERISKCODE     ,
         rec_ply_share.RISKCODE       ,
         rec_ply_share.UWYEAR         ,
         rec_ply_share.SECTNO         ,
         rec_ply_share.Ricurr       ,
         decode(rec_plan.PAYNo,1,rec_ply_share.ChgRISum,0)  ,
         rec_ply_share.ChgRIPREM *rec_plan.planrate    ,
         rec_ply_share.ChgRICOMM *rec_plan.planrate    ,
         GRBillDeal.get_bill_date(rec_plan.plandate)              ,
         rec_ply_share.startdate      ,
         rec_ply_share.ENDDATE        ,
         rec_ply_share.REstartdate    ,
         rec_ply_share.REENDDATE      ,
         'N' ,                   -- �����ʵ��� Y/N
         rec_ply_share.businessInd,
         v_endortimes,
         rec_ply.INSTMARK,
         to_char(v_paydate,'YYYYMM'),
         rec_ply_share.ACCCHANNEL,
         rec_ply_share.PlanCode,
         rec_ply_share.netind,
         rec_ply_share.sharerate,
         rec_ply_share.exchrate,
         rec_ply.currency,
         rec_ply.chgpml,
         rec_ply.chggrsprem*rec_plan.planrate,
         rec_ply.chgnetprem*rec_plan.planrate,
         sysdate,
         sysdate,
         rec_ply_share.changetax * rec_plan.planrate,--modify by wangmx 20170728
         rec_ply.taxind
      from dual;
    end loop;

    --����Լ�Է��ڱ��ѽ��е�������ֵ����1�����쳣
    if rec_ply.INSTMARK='Y' then
      select sum(premium)
        into v_sum_premium
        from REINS_RE_PLAN
       where repolicyno = rec_ply_share.repolicyno
         and reendortimes = rec_ply_share.reendortimes
         and rectimes = rec_ply_share.rectimes
         and ttyid = rec_ply_share.ttyid;
      if abs(rec_ply_share.chgriprem-v_sum_premium)>=1 then
        g_errmsg:='��Լ���ڱ���֮����ֱ����������1';
        select '*' into v_char from dual where 1=2;
      elsif abs(rec_ply_share.chgriprem-v_sum_premium)>=0.01 then
        update REINS_RE_PLAN
           set premium = premium + (rec_ply_share.chgriprem - v_sum_premium)
         where repolicyno = rec_ply_share.repolicyno
           and reendortimes = rec_ply_share.reendortimes
           and rectimes = rec_ply_share.rectimes
           and ttyid = rec_ply_share.ttyid
           and paytimes=1;
      end if;
    end if;
  end loop;

  update REINS_REENDOR set checkind='1',checkdate=v_paydate,date_updated = sysdate
   where repolicyno=p_repolicyno
     and reendortimes=p_reendortimes
     and rectimes=p_rectimes;

  exception when others then
  --������־��Ϣ
   g_errmsg:=g_errmsg||'-'|| substr(sqlerrm,1,100);
   rollback;
   insert into REINS_ENDOR_ERR_LOG(errtype,repolicyno,reendortimes,rectimes,errmsg)
         values('C2',p_repolicyno,p_reendortimes,p_rectimes,g_errmsg);

end reendor_check;

--���ⰸ���ˣ��η���ֻ���ú�Լ���ⰸ
procedure reClaim_check(p_ReClaimNo in REINS_RECLAIM.ReClaimNo%type,
                        p_rectimes in REINS_RECLAIM.rectimes%type,
                        p_messagecode out varchar2
                        ) is
 v_reinstype REINS_REPOLICY.reinstype%type;
 v_uwenddate date;
 v_paydate date;
begin
  g_errcode:='C3001';
  g_errmsg :='���ⵥ����';
  select reinstype,uwenddate into v_reinstype,v_uwenddate
   from REINS_RECLAIM
   where reClaimno=p_reClaimno
     and rectimes=p_rectimes  for update;
  if v_reinstype='0' then
     v_paydate := grbilldeal.get_bill_date(v_uwenddate);
     update REINS_RECLAIM set checkind='1',
                          checkdate=v_paydate,
                          date_updated = sysdate
      where reClaimno=p_reClaimno
        and rectimes=p_rectimes;
    --Ӧ����ʱ��Ϊ�ʵ���ǰ�ڼ���ⰸ�Ḷʱ��Ľϴ�ֵ
    update REINS_RECLAIM_SHARE set paydate=v_paydate,
                                   date_updated = sysdate
      where reClaimno=p_reClaimno
        and rectimes=p_rectimes;
  end if;
  p_messagecode:='0';
  commit;

  exception when others then
  p_messagecode:='1';
   --��¼������Ϣ
   g_errmsg:=g_errmsg||'-'|| substr(sqlerrm,1,100);
   rollback;
   insert into REINS_CLAIM_ERR_LOG(errtype,reclaimno,rectimes,errmsg)
                      values('C3',p_reclaimno,p_rectimes,g_errmsg);
end reClaim_check;

/*ȡȱʡ�ķֱ��ƻ�������Ͷ����ȡ�ֱ�����������ȵķֱ��ƻ������ڷ�����
  ����������,ȡ������Ч�ķ������ķֱ��ƻ�*/
procedure get_deft_plan(p_POLICYNO      in REINS_POLICY_UNIT.policyno%type,
                        p_DANGERUNITNO  in REINS_POLICY_UNIT.DANGERUNITNO%type,
                        p_reriskcode    in REINS_POLICY_UNIT.reriskcode%type,
                        p_riskcode      in REINS_POLICY_UNIT.riskcode%type,
                        p_restartdate   in REINS_REPOLICY.restartdate%type,
                        p_adjustflag    out REINS_REPOLICY.Flag%type,
                        p_replan        out arr_replan) is
cursor cur_plyplan is
-- Mantis 0000990 wuwp 2016-06-20 begin
select a.PriorityNo,a.ttyid,a.UWYEAR,a.TTYCODE,b.ttyname,a.openInd,0 LimitValue,0 ricommrate,0 sharerate  -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼��
     from REINS_TTY_PLAN a,REINS_TREATY b,REINS_TTY_TABLE c
     where p_restartdate between a.startdate and a.enddate
       and (b.ttyid in (select distinct c.ttyid
                    from REINS_TTY_SECT_RISK c
                   where c.reriskcode = p_reriskcode ) or
        c.ttytype in ('91', '92', '81', '82'))
       and b.ttycode=c.ttycode
       and a.ttyid=b.ttyid;
-- Mantis 0000990 wuwp 2016-06-20 end
rec_plyplan cur_plyplan%rowtype;

cursor cur_replyplan(p_repolicyno REINS_REPOLICY.repolicyno%type,
                     p_reendortimes REINS_REPOLICY.reendortimes%type) is
 select distinct PriorityNo,ttyid,UWYEAR,TTYCODE,ttyname,openInd,LimitValue,ricommrate,sharerate    -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼��
  from REINS_REPLY_PLAN_ADJ
  where repolicyno=p_repolicyno
    and reendortimes=p_reendortimes;
rec_replyplan cur_replyplan%rowtype;

cursor cur_plyunitplan(p_policyno REINS_REPOLICY.policyno%type,
                       p_dangerunitno REINS_REPOLICY.dangerunitno%type,
                       p_riskcode     REINS_REPOLICY.riskcode%type,
                       p_reriskcode   REINS_REPOLICY.reriskcode%type,
                       p_endortimes   REINS_REPOLICY.endortimes%type) is
 select distinct PriorityNo,ttyid,UWYEAR,TTYCODE,ttyname,openInd,LimitValue,ricommrate,sharerate    -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼��
  from REINS_POLICY_PLAN_ADJ
  where policyno=p_policyno
    and dangerunitno=p_dangerunitno
    and riskcode=p_riskcode
    and reriskcode=p_reriskcode
    and endortimes=p_endortimes;
rec_plyunitplan cur_plyunitplan%rowtype;

v_endortimes varchar2(3);
v_ntmp number(3):=0;
v_ntmpPly number(4):=0;
v_repolicyno   REINS_REPOLICY.repolicyno%type;
v_reendortimes REINS_REPOLICY.reendortimes%type;
begin
 p_adjustflag:='0';
 if p_policyno is not null then
  begin
   select repolicyno,reendortimes into
          v_repolicyno,v_reendortimes
     from REINS_REPOLICY
     where policyno=p_policyno
       and dangerunitno=p_dangerunitno
       and reriskcode=p_reriskcode
       and riskcode=p_riskcode
       and reinstype='0' and rownum = 1;
    exception when others then
    --dbms_output.put_line('---------'||sqlerrm);
      v_repolicyno:=null;
      v_reendortimes:=null;
   end;

  if v_repolicyno is not null then
   begin --ȡ������Ч�ķ����������Ĵ���
   select reendortimes into v_reendortimes
     from
   ( select '000' reendortimes
       from REINS_REPOLICY
      where repolicyno=v_repolicyno
        and restartdate<=p_restartdate
        and reenddate>=p_restartdate
        and status in ('0','1','2')
       union all
       select reendortimes
       from REINS_REENDOR
      where repolicyno=v_repolicyno
        and restartdate<=p_restartdate
        and reenddate>=p_restartdate
        and status in ('0','1','2') ) aa;
    exception when others then
      v_reendortimes:='000';
    end;
  end if;

  select count(*) into v_ntmp
    from REINS_REPLY_PLAN_ADJ
   where repolicyno=v_repolicyno
     and reendortimes=v_reendortimes;

  select count(*) into v_ntmpPly
    from REINS_POLICY_PLAN_ADJ
  where policyno=p_policyno
   and dangerunitno=p_DANGERUNITNO
   and riskcode = p_riskcode
   and reriskcode = p_reriskcode;
 end if; --�����Ų���


  if v_ntmp=0 and v_ntmpPly=0  then
   for rec_plyplan in cur_plyplan loop
    v_ntmp:=v_ntmp+1;
    p_replan(v_ntmp).PriorityNo:=rec_plyplan.PriorityNo;
    p_replan(v_ntmp).ttyid:=rec_plyplan.ttyid;
    p_replan(v_ntmp).UWYEAR:=rec_plyplan.UWYEAR;
    p_replan(v_ntmp).TTYCODE:=rec_plyplan.TTYCODE;
    p_replan(v_ntmp).ttyname:=rec_plyplan.ttyname;
    p_replan(v_ntmp).openInd:=rec_plyplan.openInd;
    p_replan(v_ntmp).LimitValue:=rec_plyplan.LimitValue;
    p_replan(v_ntmp).ricommrate:=rec_plyplan.ricommrate;  -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼��
    p_replan(v_ntmp).shareRate:=rec_plyplan.shareRate;--modify by wangmx 2018-07-06
   end loop;
  else
   p_adjustflag:='1';
  if v_ntmp>0 then
    v_ntmp:=0;
   for rec_replyplan in cur_replyplan(v_repolicyno,v_reendortimes) loop
   v_ntmp:=v_ntmp+1;
    p_replan(v_ntmp).PriorityNo:=rec_Replyplan.PriorityNo;
    p_replan(v_ntmp).ttyid:=rec_Replyplan.ttyid;
    p_replan(v_ntmp).UWYEAR:=rec_Replyplan.UWYEAR;
    p_replan(v_ntmp).TTYCODE:=rec_Replyplan.TTYCODE;
    p_replan(v_ntmp).ttyname:=rec_Replyplan.ttyname;
    p_replan(v_ntmp).openInd:=rec_Replyplan.openInd;
    p_replan(v_ntmp).LimitValue:=rec_Replyplan.LimitValue;
    p_replan(v_ntmp).ricommrate:=rec_Replyplan.ricommrate;   -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼��
    p_replan(v_ntmp).shareRate:=rec_Replyplan.shareRate;--modify by wangmx 2018-07-06
   end loop;
   else
     begin
      v_ntmpPly:=0;
      select max(endortimes) into v_endortimes
      from REINS_POLICY_PLAN_ADJ
      where policyno=p_policyno
        and dangerunitno=p_dangerunitno
        and riskcode=p_riskcode
        and reriskcode=p_reriskcode;
     for rec_plyunitplan in cur_plyunitplan(p_policyno,p_dangerunitno,p_riskcode,p_reriskcode,v_endortimes)loop
         v_ntmpPly:=v_ntmpPly+1;
         p_replan(v_ntmpPly).PriorityNo:=rec_plyunitplan.PriorityNo;
         p_replan(v_ntmpPly).ttyid:=rec_plyunitplan.ttyid;
         p_replan(v_ntmpPly).UWYEAR:=rec_plyunitplan.UWYEAR;
         p_replan(v_ntmpPly).TTYCODE:=rec_plyunitplan.TTYCODE;
         p_replan(v_ntmpPly).ttyname:=rec_plyunitplan.ttyname;
         p_replan(v_ntmpPly).openInd:=rec_plyunitplan.openInd;
         p_replan(v_ntmpPly).LimitValue:=rec_plyunitplan.LimitValue;
         p_replan(v_ntmpPly).ricommrate:=rec_plyunitplan.ricommrate;   -- modify by wuwp 2016-06-25 �ֱ��ƻ������������¼��
         p_replan(v_ntmpPly).shareRate:=rec_plyunitplan.shareRate;
     end loop;
     exception
       when others then
         null;
     end;
  end if;
 end if;
 for r in 1.. p_replan.count loop
     dbms_output.put_line('-------'||p_replan(r).ttyid||'----'||p_replan(r).sharerate);
 end loop;
end get_deft_plan;

--�Ӽ�������ȡ�����ķֳ�����
function get_retent(p_arr_re_abs in arr_re_abs )
return REINS_REPLY_SHARE.RISum%type  is
begin
   for i in 1..p_arr_re_abs.count loop
     if p_arr_re_abs(i).ttytype='81' then
      return p_arr_re_abs(i).RISum;
     end if;
  end loop;
  return 0;
  exception when others then
   return 0;
end get_retent;

--�ӷֳ�����м���Σ�յ�λ���ۼ�������
function get_c_retent(p_reply_para in reply_para)
return REINS_REPLY_SHARE.RISum%type is
cursor cur_c_retent is
    select ricurr,sum(RISum) risum
     from REINS_REPLY_SHARE t
    where DangerCode=p_reply_para.DangerCode
      and not exists (select 1 from REINS_REPLY_SHARE a where a.repolicyno=t.repolicyno and a.repolicyno = nvl(p_reply_para.repolicyno,'*'))
      and restartdate<=sysdate
      and reenddate>=sysdate
      and dangertype=p_reply_para.dangertype
      and (ttytype='81' or ttytype='21')
    group by ricurr
  union all
    select ricurr,sum(RISum) risum
     from REINS_REENDR_SHARE t
    where DangerCode=p_reply_para.DangerCode
      and not exists (select 1 from REINS_REENDR_SHARE a where a.repolicyno=t.repolicyno and a.repolicyno = nvl(p_reply_para.repolicyno,'*'))
      and restartdate<=sysdate
      and reenddate>=sysdate
      and dangertype=p_reply_para.dangertype
      and (ttytype='81' or ttytype='21')
    group by ricurr;
rec_c_retent cur_c_retent%rowtype;


v_cur_retent REINS_REPLY_SHARE.RISum%type:=0;
v_tot_retent REINS_REPLY_SHARE.RISum%type:=0;  --Σ�յ�λ����ָ������������
begin

  if p_reply_para.Dangertype in ('A','B','C') then

   for rec_c_retent in cur_c_retent loop
     v_cur_retent:=v_cur_retent+rec_c_retent.risum*get_exchrate(rec_c_retent.ricurr,p_reply_para.oricurr,sysdate);
   end loop;


   v_tot_retent:=get_danger_limit(p_reply_para);

    if v_tot_retent-v_cur_retent<0.1 then --���ȿ���
      return 0;
    else
      return v_tot_retent-v_cur_retent;
    end if;

  else
    return p_reply_para.retentvalue;
  end if;


  exception when others then
   return p_reply_para.OriPML;
end get_c_retent;

function get_yylimit(p_reply_para in reply_para,
                     p_ttyid in REINS_TREATY.ttyid%type,
                     p_currency in REINS_REPOLICY.currency%type)
return REINS_REPLY_SHARE.RISum%type is
cursor cur_limit is
    select ricurr,sum(RISum) risum
     from REINS_REPLY_SHARE
    where DangerCode=p_reply_para.DangerCode
      and repolicyno<>nvl(p_reply_para.repolicyno,'*')
      and restartdate<=sysdate
      and reenddate>=sysdate
      and ttyid=p_ttyid
      and dangertype=p_reply_para.DangerType
    group by ricurr
  union all
    select ricurr,sum(RISum) risum
     from REINS_REENDR_SHARE
    where DangerCode=p_reply_para.DangerCode
      and repolicyno<>nvl(p_reply_para.repolicyno,'*')
      and restartdate<=sysdate
      and reenddate>=sysdate
      and ttyid=p_ttyid
      and dangertype=p_reply_para.DangerType
    group by ricurr;
rec_limit cur_limit%rowtype;

v_limit REINS_REPLY_SHARE.RISum%type:=0;

begin

   for rec_limit in cur_limit loop
     v_limit:=v_limit+rec_limit.risum*get_exchrate(rec_limit.ricurr,p_currency,sysdate);
   end loop;
   return v_limit;
end get_yylimit;

function get_danger_limit(p_reply_para in reply_para )
return REINS_REPLY_SHARE.RISum%type is
v_cur_retent REINS_REPLY_SHARE.RISum%type:=0;
begin
   if p_reply_para.Dangertype='A' then
     select RETENTIONVALUE*get_exchrate(currency,p_reply_para.oricurr,sysdate)
            into v_cur_retent
       from PUB_BLOCK_RISK
      where blockcode=substrb(p_reply_para.DangerCode,1,12)
        and riskkind=substrb(p_reply_para.DangerCode,length(p_reply_para.DangerCode)-1,2)
        and validind='1'
        and rownum=1;
   end if;
   if p_reply_para.Dangertype='B' then
     select linevalue*get_exchrate(linecurrency,p_reply_para.oricurr,sysdate)
            into v_cur_retent
       from PUB_VOYAGE
      where voyagecardNo= p_reply_para.DangerCode and validind='1';
   end if;
   if p_reply_para.Dangertype='C' then
     select distinct RETENTIONVALUE*get_exchrate(currency,p_reply_para.oricurr,sysdate)
            into v_cur_retent
       from PUB_VESSEL
      where vesselcode=p_reply_para.DangerCode and validind='1';
   end if;

  return  v_cur_retent;
end get_danger_limit;

/* ȡ���µĺ�ԼID�������Լ����Ļ�������ת��Լ������Ҳ�����Լ���򷵻ؿ� */
function get_last_ttyid(p_ttyid in REINS_TREATY.ttyid%type)
    return REINS_TREATY.ttyid%type
    is
    v_TtyStatus REINS_TREATY.TtyStatus%type;
    v_TtyCode   REINS_TREATY.TtyCode%type;
    cursor cur_tty is
    select ttyid
     from REINS_TREATY where ttystatus='2'
     start with ttyid=p_ttyid
     connect by prior exttyid=ttyid;
    rec_tty cur_tty%rowtype;
    --tmp_ttyid REINS_TREATY.ttyid%type;
begin
   select TtyStatus,TtyCode into v_TtyStatus,v_TtyCode
     from REINS_TREATY
    where ttyid=p_ttyid;
   if v_ttystatus='3' then --�ѹر�
    open cur_tty;
      fetch cur_tty into rec_tty;
       if cur_tty%notfound then
          return null;
       else
        return rec_tty.ttyid;
       end if;
     close cur_tty;
   else
    return p_ttyid;  --�����Լ��Ч��ֱ�ӷ���
   end if;

  exception when others then
  return null;

end get_last_ttyid;

/* ȡ������ԼID */
function get_retent_ttyid(p_uwyear in REINS_TREATY.Uwyear%type)
    return REINS_TREATY.ttyid%type
 is
    cursor cur_tty is select TtyID
     from REINS_TREATY a,REINS_TTY_TABLE b
    where a.ttycode=b.ttycode
      and a.uwyear=p_uwyear
      and b.ttytype='81';
    tmp_ttyid REINS_TREATY.ttyid%type;
begin
    open cur_tty;
      fetch cur_tty into tmp_ttyid;
     close cur_tty;
    return tmp_ttyid;
  exception when others then
  return null;
end get_retent_ttyid;

/* ȡ����������ԼID */
function get_affi_ttyid(p_uwyear in REINS_TREATY.Uwyear%type)
    return REINS_TREATY.ttyid%type
 is
    cursor cur_tty is select TtyID
     from REINS_TREATY a,REINS_TTY_TABLE b
    where a.ttycode=b.ttycode
      and a.uwyear=p_uwyear
      and b.ttytype='82';
    tmp_ttyid REINS_TREATY.ttyid%type;
begin
    open cur_tty;
      fetch cur_tty into tmp_ttyid;
     close cur_tty;
    return tmp_ttyid;
  exception when others then
  return null;
end get_affi_ttyid;
/* ȡ�ٷֺ�ԼID */
function get_fac_ttyid(p_uwyear in REINS_TREATY.Uwyear%type)
    return REINS_TREATY.ttyid%type
 is
    cursor cur_tty is select TtyID
     from REINS_TREATY a,REINS_TTY_TABLE b
    where a.ttycode=b.ttycode
      and a.uwyear=p_uwyear
      and b.ttycode = 'FAC01'
      and b.ttytype='91';
    tmp_ttyid REINS_TREATY.ttyid%type;
begin
    open cur_tty;
      fetch cur_tty into tmp_ttyid;
     close cur_tty;
    return tmp_ttyid;
  exception when others then
  return null;
end get_fac_ttyid;

/* ȡ�Ǳ����ٷֺ�ԼID */
function get_nfac_ttyid(p_uwyear in REINS_TREATY.Uwyear%type)
    return REINS_TREATY.ttyid%type
 is
    cursor cur_tty is select TtyID
     from REINS_TREATY a,REINS_TTY_TABLE b
    where a.ttycode=b.ttycode
      and a.uwyear=p_uwyear
      and b.ttycode = 'FAC02'
      and b.ttytype='92';
    tmp_ttyid REINS_TREATY.ttyid%type;
begin
    open cur_tty;
      fetch cur_tty into tmp_ttyid;
     close cur_tty;
    return tmp_ttyid;
  exception when others then
  return null;
end get_nfac_ttyid;

/* ȡ���������ҵ��ֳ����Ʊ��� */--liuxd add 2009.6.26
function get_ttyid_discount(p_type in varchar2, p_ttyid in REINS_TREATY.ttyid%type, p_reply_para in reply_para)
return REINS_TTY_SECT.InLimitRateLower%type
 is
    cursor cur_tty is
      select a.fCoiLIMITBase, a.fCoilimitInd, nvl(a.fCoiLimitRate,0) fCoiLimitRate,
             a.mCoiLIMITBase, a.mCoilimitInd, nvl(a.mCoiLimitRate,0) mCoiLimitRate,
             a.InLIMITBase, a.InLimitInd, nvl(a.InLimitRateLower,0) InLimitRateLower, nvl(a.InLimitRateUpper,0) InLimitRateUpper
        from REINS_TTY_SECT a, REINS_TTY_SECT_RISK b
       where a.ttyid=p_ttyid
         and a.ttyid=b.ttyid and a.sectno=b.sectno
         and b.reriskcode=p_reply_para.reriskcode
         --add by 2013-07-11 �Ƿ�ũ������,��ҵ������
         --and a.industry_class=p_reply_para.industryClass;
         --modified by huangxf 2013/08/24 ����ͨ�������
         AND a.industry_class = DECODE(a.industry_class, '*', '*', p_reply_para.industryclass);

    rec_tty     cur_tty%rowtype;
    n_discount  REINS_TTY_SECT.InLimitRateLower%type:=100;
    v_char      char(1);
begin
   open cur_tty;
      fetch cur_tty into rec_tty;
      if cur_tty%notfound then
        select '*' into v_char from dual where 1=2;
      end if;
   close cur_tty;

--ȡ����ҵ��ֳ����Ʊ���
   if p_reply_para.CoinsInd='1' and rec_tty.mCoilimitInd='1' then   --����
      if (p_type='0' and rec_tty.MCoiLIMITBase='2') or   --������
         (p_type='0' and rec_tty.MCoiLIMITBase='1') or   --���޶�
         (p_type='0' and rec_tty.MCoiLIMITBase='3') or   --
         (p_type='1' and rec_tty.MCoiLIMITBase='3') or   --������
         (p_type='2' and rec_tty.MCoiLIMITBase='1') then      --������Լ�޽�����������֮�֣��������ӹ�֮��
        n_discount := rec_tty.mCoiLimitRate;
      end if;
   elsif p_reply_para.CoinsInd='2' and rec_tty.fCoilimitInd='1' then  --�ӹ�
      if (p_type='0' and rec_tty.FCoiLIMITBase='2') or   --������
         (p_type='0' and rec_tty.FCoiLIMITBase='1') or   --���޶�
         (p_type='0' and rec_tty.FCoiLIMITBase='3') or
         (p_type='1' and rec_tty.FCoiLIMITBase='3') or   --������
         (p_type='2' and rec_tty.FCoiLIMITBase='1') then      --������Լ�޽�����������֮�֣��������ӹ�֮��
        n_discount := rec_tty.fCoiLimitRate;
      end if;
   end if;

--ȡ����ҵ��ֳ����Ʊ���
   if p_reply_para.BusinessInd<>'0' and rec_tty.InLimitInd='1' then  --����̶��ۿ�
      if (p_type='0' and rec_tty.InLIMITBase='2') or   --������
         (p_type='0' and rec_tty.InLIMITBase='1') or   --���޶�
         (p_type='0' and rec_tty.InLIMITBase='3') or
         (p_type='1' and rec_tty.InLIMITBase='3') or   --������
         (p_type='2' and rec_tty.InLIMITBase='1') then      --������Լ�޽�����������֮��
        if p_reply_para.BaseRate>=rec_tty.InLimitRateLower and p_reply_para.BaseRate<=rec_tty.InLimitRateUpper  then
          n_discount := p_reply_para.BaseRate;
        elsif p_reply_para.BaseRate<rec_tty.InLimitRateLower then
          n_discount := rec_tty.InLimitRateLower;
        else
          n_discount := rec_tty.InLimitRateUpper;
        end if;
      end if;
   end if;

   return n_discount;

  exception when others then
    return 100;
end get_ttyid_discount;

/* ��������ʱ��ǰ�ж��Ƿ������������򷵻����ttyid */ --liuxd add 2009.6.26
function Judge_Surplus(p_type          in varchar2,
                       p_reply_para    in reply_para)
return REINS_TREATY.ttyid%type
is
--��Լ�ֱ��ƻ�
cursor cur_ttyplan(p_planadj_flag varchar2)
 is
 select ttyid
 from
   (select a.ttyid
     from REINS_TTY_PLAN a,REINS_TREATY b ,REINS_TTY_TABLE c,REINS_TTY_SECT_RISK d
     where p_reply_para.startdate between a.startdate and a.enddate
       and a.ttyid=b.ttyid
       and b.ttycode=c.ttycode
       and b.ttystatus in ('2','3')
       and c.ttytype = '31'   --����Լ
       and b.ttyid=d.ttyid
       and d.reriskcode=p_reply_para.ReRiskCode
       --and b.startdate<=p_reply_para.restartdate
       and p_planadj_flag='N'
       --add by 2013-07-11��Լ��Ҫ�����Ƿ�ũ����Y��N��
       --and b.channel_class=p_reply_para.channelClass
       --modified by huangxf 2013/08/24 ����ͨ�������
       AND b.channel_class = DECODE(b.channel_class, '*', '*', p_reply_para.channelClass)
   union all
   select a.ttyid
     from REINS_PROP_PLAN_ADJ a,REINS_TREATY b,REINS_TTY_TABLE c,REINS_TTY_SECT_RISK d
    where certino=p_reply_para.certino
      and DangerUnitNo=p_reply_para.DangerUnitNo
      and a.ReRiskCode=p_reply_para.ReRiskCode
      and RiskCode=p_reply_para.RiskCode
      and a.ttyid=b.ttyid
      and b.ttycode=c.ttycode
      --and b.startdate<=p_reply_para.restartdate
      and b.ttystatus in ('2','3')
      and c.ttytype = '31'   --����Լ
      and p_type='0'
      and p_planadj_flag='Y'
      and a.ttyid=d.ttyid
      and d.reriskcode=p_reply_para.ReRiskCode
      --add by 2013-07-11��Լ��Ҫ�����Ƿ�ũ����Y��N��
      --and b.channel_class=p_reply_para.channelClass
      --modified by huangxf 2013/08/24 ����ͨ�������
      AND b.channel_class = DECODE(b.channel_class, '*', '*', p_reply_para.channelClass)
    union all
   select a.ttyid
     from REINS_REPLY_PLAN_ADJ a,REINS_TREATY b,REINS_TTY_TABLE c,REINS_TTY_SECT_RISK d
    where repolicyno=p_reply_para.repolicyno
      and ReEndortimes=p_reply_para.ReEndortimes
      and RecTimes = p_reply_para.RecTimes
      and a.ttyid=b.ttyid
      and b.ttycode=c.ttycode
      and b.ttyid=d.ttyid
      and d.reriskcode=p_reply_para.ReRiskCode
      --and b.startdate<=p_reply_para.restartdate
      and b.ttystatus in ('2','3')
      and c.ttytype = '31'   --����Լ
      and p_type='1'
      and p_planadj_flag='Y'
      --add by 2013-07-11��Լ��Ҫ�����Ƿ�ũ����Y��N��
      --and b.channel_class=p_reply_para.channelClass
      --modified by huangxf 2013/08/24 ����ͨ�������
      AND b.channel_class = DECODE(b.channel_class, '*', '*', p_reply_para.channelClass)
     ) aa;
     --order by PriorityNo;
  rec_ttyplan cur_ttyPlan%rowtype;
  n_cnt1   number(5):=0;
  v_planadj_flag varchar2(1):='N'; --�Ƿ���ڷֱ��ƻ�����
  tmp_ttyid  varchar2(20):=null;
  v_char varchar2(1);
begin

   if p_type='0' then
     select count(*) into n_cnt1 from REINS_PROP_PLAN_ADJ
      where certino=p_reply_para.certino
        and DangerUnitNo=p_reply_para.DangerUnitNo
        and ReRiskCode=p_reply_para.ReRiskCode
        and RiskCode=p_reply_para.RiskCode;
   elsif p_type='1' then
     select count(*) into n_cnt1 from REINS_REPLY_PLAN_ADJ
      where RePolicyNo=p_reply_para.RePolicyNo
        and reendortimes=p_reply_para.reendortimes;
   end if;

   if n_cnt1>0 then
     v_planadj_flag:='Y';
   end if;

   open cur_ttyplan(v_planadj_flag);
    fetch cur_ttyplan  into rec_ttyplan;
    if cur_ttyplan%found then
      tmp_ttyid:=rec_ttyplan.ttyid;
    else
      select '*' into v_char from dual where 1=2;
    end if;
   close cur_ttyplan;
   return tmp_ttyid;

  exception when others then
  return null;
end Judge_Surplus;

/* �Ǳ����ٷַ������󣬻�д��Ӧ�Ļָ����Ѻ͵�ǰ�����޶� */
procedure rewrite_nfac_ply(p_reclaimno in REINS_RECLAIM.reclaimno%type,
                           p_RecTimes  in REINS_RECLAIM.RecTimes%type,
                           p_reclm_para in reclm_para) is
  cursor cur_nfac_clm is
        select layerno,RENPREM,PAIDSUM
        from REINS_RECLM_N_FAC
        where RECLAIMNO=p_reclaimno
        and RECTIMES=p_RecTimes;
  rec_nfac_clm  cur_nfac_clm%rowtype;
begin

  for rec_nfac_clm in cur_nfac_clm loop
     update REINS_REPLY_N_FAC a set premium = rec_nfac_clm.RENPREM,
                              CURCONTQUOTA = CONTQUOTA*(RECTIMES+1)-rec_nfac_clm.PAIDSUM
      where (a.RepolicyNo,a.RecTimes)
         in (select RepolicyNo, RecTimes
             from REINS_REPOLICY b
             where policyNo=p_reclm_para.policyno
              and b.DANGERUNITNO=p_reclm_para.DANGERUNITNO
              and b.riskcode=p_reclm_para.riskcode
              and b.reriskcode=p_reclm_para.reriskcode
              and b.restartdate<=p_reclm_para.DamageDATE
              and b.reenddate>=p_reclm_para.DamageDATE
              and b.Status='1'
              and b.reinstype='2')
          and a.layerno = rec_nfac_clm.layerno;

     update REINS_REENDR_N_FAC a set premium=rec_nfac_clm.RENPREM,
                               CURCONTQUOTA = CONTQUOTA*(RECTIMES+1)-rec_nfac_clm.PAIDSUM
      where (a.RepolicyNo,a.RecTimes,a.reendortimes)
         in (select RepolicyNo,RecTimes,reendortimes
             from REINS_REENDOR b
             where policyNo=p_reclm_para.policyno
              and b.DANGERUNITNO=p_reclm_para.DANGERUNITNO
              and b.riskcode=p_reclm_para.riskcode
              and b.reriskcode=p_reclm_para.reriskcode
              and b.restartdate<=p_reclm_para.DamageDATE
              and b.reenddate>=p_reclm_para.DamageDATE
              and b.Status='1'
              and b.reinstype='2')
          and a.layerno = rec_nfac_clm.layerno;
  end loop;

end rewrite_nfac_ply;

/* �ϴ���Ч������ */
function get_last_EndorTimes(p_repolicyno   in REINS_REPOLICY.repolicyno%type,
                             p_reendortimes in REINS_REPOLICY.reendortimes%type,
                             p_restartdate  in REINS_REPOLICY.Restartdate%type
)
 return REINS_REENDOR.Reendortimes%type is
 v_Reendortimes  REINS_REENDOR.Reendortimes%type;
 v_ntmp number(3);
begin
select max(reendortimes) into  v_Reendortimes
  from REINS_REENDOR
 where repolicyno=p_repolicyno
   and Reendortimes<p_Reendortimes
   --and restartDate<=p_reStartDate  --modify by liupeng 2012-03-28
   and status not in ('3','4','5');

   if v_Reendortimes is null then
    select count(*) into  v_ntmp
      from REINS_REPOLICY
     where repolicyno=p_repolicyno
       and status not in ('3','4','5');
       if v_ntmp>0 then
          v_Reendortimes:='000';
       else
          v_Reendortimes:='-1';
       end if;
   end if;

  return v_Reendortimes;
  exception when others then
  return -1;

end get_last_EndorTimes;

function get_danger_PML(p_DangerCode in REINS_REPOLICY.repolicyno%type,
                         p_ricurr     in REINS_REPOLICY.currency%type,
                         p_date       in date ,
                         p_dangertype in REINS_REPOLICY.dangertype%type)
    return REINS_REPLY_SHARE.RISum%type is
 cursor cur_Danger is
   select currency,PML*sharerate/100 PML
     from REINS_REPOLICY
    where DangerCode=p_DangerCode
      and restartdate<=p_DATE
      and reenddate>=p_DATE
      and Status in ('0','1','2')
      and dangertype=p_dangertype
   union
    select currency,PML*sharerate/100 PML
     from REINS_REENDOR
    where DangerCode=p_DangerCode
      and restartdate<=p_DATE
      and reenddate>=p_DATE
      and Status in ('0','1','2')
      and dangertype=p_dangertype;
 rec_Danger cur_Danger%rowtype;
 v_tot_pml REINS_REPLY_SHARE.RISum%type:=0;
 begin
   for rec_danger in cur_danger loop
   v_tot_pml:=v_tot_pml+rec_danger.pml*get_exchrate(rec_danger.currency,p_ricurr,p_date);
  end loop;
  return v_tot_pml;
end get_danger_PML;

function get_danger_c_PML(p_DangerCode in REINS_REPOLICY.repolicyno%type,
                          p_ricurr     in REINS_REPOLICY.currency%type,
                          p_date       in date,
                          p_dangertype in varchar2)
  return REINS_REPLY_SHARE.RISum%type is
  cursor cur_danger is
    select a.oricurr,sum(a.RISum/exchrate) risum
     from REINS_REPLY_SHARE a,REINS_REPOLICY b
    where b.DangerCode=p_DangerCode
      and b.restartdate<=p_date
      and b.reenddate>=p_date
      and a.PCInd='C'
      and a.repolicyno=b.repolicyno
      and a.rectimes=b.rectimes
      and a.dangertype=p_dangertype
    group by oricurr
  union all
    select a.oricurr,sum(RISum/exchrate) risum
     from REINS_REENDR_SHARE a,REINS_REENDOR b
    where b.DangerCode=p_DangerCode
      and b.restartdate<=p_date
      and b.reenddate>=p_date
      and a.PCInd='C'
      and a.repolicyno=b.repolicyno
      and a.rectimes=b.rectimes
      and a.reendortimes=b.reendortimes
      and a.dangertype=p_dangertype
    group by oricurr;
 rec_Danger cur_Danger%rowtype;
 v_tot_pml REINS_REPLY_SHARE.RISum%type:=0;
 begin
  for rec_danger in cur_danger loop
   v_tot_pml:=v_tot_pml+rec_danger.risum*get_exchrate(rec_danger.oricurr,p_ricurr,p_date);
  end loop;
  return v_tot_pml;
 end get_danger_c_PML;

function get_danger_RiSum(p_reply_para in reply_para,p_ttyid in REINS_REPLY_SHARE.ttyid%type,p_dangertype in REINS_REPLY_SHARE.dangertype%type)
return REINS_REPLY_SHARE.RiSum%type is
  cursor cur_danger_riSum is
    select ricurr,sum(RISum) risum
     from REINS_REPLY_SHARE
    where DangerCode=p_reply_para.DangerCode
      and repolicyno<>nvl(p_reply_para.repolicyno,'*')
      and restartdate<=sysdate
      and reenddate>=sysdate
      and ttyID=p_ttyid
      and dangertype=p_dangertype
    group by ricurr
  union all
    select ricurr,sum(RISum) risum
     from REINS_REENDR_SHARE
    where DangerCode=p_reply_para.DangerCode
      and repolicyno<>nvl(p_reply_para.repolicyno,'*')
      and restartdate<=sysdate
      and reenddate>=sysdate
     and ttyID=p_ttyid
     and dangertype=p_dangertype
    group by ricurr;
rec_danger_riSum cur_danger_RiSum%rowtype;
v_cur_riSum REINS_REPLY_SHARE.RISum%type:=0;
begin
   for rec_danger_riSum in cur_danger_riSum loop
     v_cur_riSum:=v_cur_riSum+rec_danger_riSum.risum*get_exchrate(rec_danger_riSum.ricurr,p_reply_para.oricurr,sysdate);
   end loop;
 return v_cur_riSum;
end  get_danger_RiSum;

function get_exchrate(p_oricurr in REINS_TTY_CURR.Currency%type,
                      p_curcurr in REINS_TTY_CURR.Currency%type,
                      p_date date)
return REINS_REPLY_SHARE.ExchRate%type
  is
 v_exchrate PUB_EXCH.EXCHRATE%type;
 v_bCurrExchRate PUB_EXCH.EXCHRATE%type;
 v_eCurrExchRate PUB_EXCH.EXCHRATE%type;
 begin
   v_exchrate:=get_exchrateBase(p_oricurr,p_curcurr,p_date);
   if v_exchrate is null then
     v_exchrate:=get_exchrateBase(p_curcurr,p_oricurr,p_date);
     if v_exchrate is null then
        v_bCurrExchRate:=get_exchrateBase('CNY',p_curcurr,p_date);
        v_eCurrExchRate:=get_exchrateBase('CNY',p_oricurr,p_date);
        v_exchrate:=v_eCurrExchRate/v_bCurrExchRate;
        return v_exchrate;
     else
        return 1/v_exchrate;
     end if;
   else
     return v_exchrate;
   end if;
 end get_exchrate;

function get_exchrateBase(p_oricurr in REINS_TTY_CURR.Currency%type,
                          p_curcurr in REINS_TTY_CURR.Currency%type,
                          p_date date)
 return REINS_REPLY_SHARE.ExchRate%type
  is
 v_date date;
 v_exchrate PUB_EXCH.EXCHRATE%type;
 v_char char(1);
 begin
   if p_oricurr=p_curcurr then
    return 1;
   else
    select max(exchdate) into v_date from PUB_EXCH
    where exchdate<=p_date and basecurrency=p_oricurr and exchCurrency=p_curcurr and exchtype='1';
    if v_Date is null then
     select '*' into v_char from dual where 1=2;
    end if;
    select EXCHRATE into v_exchrate
      from PUB_EXCH
     where exchdate=v_date
       and basecurrency=p_oricurr
       and exchCurrency=p_curcurr
       and exchtype='1';
     return v_exchrate;
   end if;
   exception
    when others then
      return null;
 end get_exchrateBase;

procedure get_RiskUnitPmlAndRet(p_bustype in varchar2,p_certino in varchar2,p_dangerunitno in number,p_riskcode in varchar2,p_reriskcode in varchar2,p_pml in out number,p_retention in out number) is

  cursor cur_policyUnit is
    select t1.respecialind,t1.nacccalind,t1.acccalind,t1.mainreriskcode,t.*
      from reins_code_re_risk t1,
           reins_code_re_risk t2,
           reins_policy_unit  t
     where t1.mainreriskcode = t2.mainreriskcode
       and t2.reriskcode = p_reriskcode
       and t.proposalno = p_certino
       and t.dangerunitno = p_dangerunitno
       and t.riskcode = p_riskcode
       and t.reriskcode = t1.reriskcode;
    rec_policyUnit cur_policyUnit%rowtype;

  cursor cur_endorUnit is
    select t.*,t1.amountgroundrate,t3.respecialind,t3.nacccalind,t3.acccalind,t3.mainreriskcode
      from reins_code_re_risk t3,
           reins_code_re_risk t2,
           reins_endor_unit   t,
           reins_policy_unit  t1
     where t3.mainreriskcode = t2.mainreriskcode
       and t2.reriskcode = p_reriskcode
       and t.policyno = t1.policyno
       and t.riskcode = t1.riskcode
       and t.reriskcode = t1.reriskcode
       and t.dangerunitno = t1.dangerunitno
       and t.endorno = p_certino
       and t.dangerunitno = p_dangerunitno
       and t.riskcode = p_riskcode
       and t.reriskcode = t3.reriskcode;
    rec_endorUnit cur_endorUnit%rowtype;

begin
    if p_bustype = 'T' then
       for rec_policyUnit in cur_policyUnit loop
           if rec_policyUnit.respecialind='2' or (rec_policyUnit.respecialind='3' and rec_policyUnit.acccalind='1') then
             if rec_policyUnit.Coinsind='1' then
                 p_pml:=p_pml+rec_policyUnit.Pml*rec_policyUnit.Baserate/100/**rec_policyUnit.Amountgroundrate/100*/;
             else
                 p_pml:=p_pml+rec_policyUnit.Pml/**rec_policyUnit.Amountgroundrate/100*/;
             end if;

             if rec_policyUnit.respecialind='2' then
                 p_retention:=rec_policyUnit.Retentvalue;
             end if;
           end if;
        end loop;
    else
       for rec_endorUnit in cur_endorUnit loop
           if rec_endorUnit.respecialind='2' or (rec_endorUnit.respecialind='3' and rec_endorUnit.acccalind='1') then
             if rec_endorUnit.Coinsind='1' then
                 p_pml:=p_pml+rec_endorUnit.Pml*rec_endorUnit.Baserate/100/**rec_endorUnit.Amountgroundrate/100*/;
             else
                 p_pml:=p_pml+rec_endorUnit.Pml/**rec_endorUnit.Amountgroundrate/100*/;
             end if;

             if rec_endorUnit.respecialind='2' then
                 p_retention:=rec_endorUnit.Retentvalue;
             end if;
           end if;
       end loop;
    end if;
end get_RiskUnitPmlAndRet;

procedure modify_netprem(p_policyno in REINS_REPOLICY.policyno%type) is
  cursor cur_reply is
    select *
     from REINS_REPOLICY
     where policyno = p_policyno and status not in ('3','4');

  rec_reply        cur_reply%rowtype;

  v_netcount       number(3):=0;
  v_commissionrate number:=0;
  v_commissionrate2 number:=0;
  --    v_servicerate     number(12, 5);
  v_businesssource  NBZ_POLICY_MAIN.businesssource%type;
  v_hengsangind     NBZ_POLICY_MAIN.hengsangind%type;
  v_frontingtaxrate NBZ_POLICY_MAIN.frontingritaxpercent%type;
  v_proposalno      NBZ_POLICY_MAIN.proposalno%type;
  v_servicerate     NBZ_POLICY_COMMISSION.commissionpercent%type:=0;
begin
    --ȡӶ�����
    select proposalno, businesssource, hengsangind, nvl(a.frontingritaxpercent,0)
      into v_proposalno, v_businesssource, v_hengsangind, v_frontingtaxrate
      from NBZ_POLICY_MAIN a
     where policyno = p_policyno;

    if v_businesssource = '0050' then
      --Frontҵ��ȡӶ�����
      select nvl(sum(commissionpercent),0)
        into v_commissionrate
        from NBZ_POLICY_COMMISSION
       where policyno = p_policyno
         and commissiontype in  ('201','202','203','204','205');
       v_commissionrate2 := v_commissionrate;
    else
      --��Frontҵ��ȡӶ�����
      select nvl(sum(sumnetpremium) * 100 / sum(sumgrosspremium),0)
        into v_commissionrate
        from NBZ_POLICY_RISK
       where policyno = p_policyno;
      v_commissionrate := 100 - v_commissionrate ;
    end if;
    --ȡFronting��Ϣ
    if (v_businesssource = '0050' and v_hengsangind != '2') then --fronting�Ǻ���

      v_commissionrate := (100 - v_frontingtaxrate - v_commissionrate) / 100;

    elsif (v_businesssource = '0050' and v_hengsangind = '2') then  --fronting����

      --ȡ��������ѱ���
      begin
        select nvl(commissionpercent, 0)
         into v_servicerate
         from NBZ_POLICY_COMMISSION
        where policyno = rec_reply.policyno
          and commissiontype = '212'
          and rownum = 1;
      exception when others then
        v_servicerate := 0;
      end;
      v_commissionrate := (100 - v_frontingtaxrate) *
                          (100 - v_commissionrate - v_servicerate) / 10000;
    else  --��frontingҵ��
      v_commissionrate := (100 - v_commissionrate) / 100;
    end if;

    --�޸�Ͷ�������������������롢����Σ�յ�λ�ľ�����
    update REINS_PROP_UNIT
       set netprem = grsprem * v_commissionrate
     where proposalno = v_proposalno;
    update REINS_PROP_UNIT_PLAN
       set netplanfee = grsplanfee * v_commissionrate
     where proposalno = v_proposalno;

    update REINS_POLICY_UNIT
       set netprem = grsprem * v_commissionrate
     where policyno = p_policyno;
    update REINS_POLICY_UNIT_PLAN
       set netplanfee = grsplanfee * v_commissionrate
     where policyno = p_policyno;

    update REINS_ENDOR_APPLY_UNIT
       set netprem    = grsprem * v_commissionrate,
           chgnetprem = chggrsprem * v_commissionrate
     where endorno in(select endorno from NBZ_ENDOR_MAIN where policyno=p_policyno);
    update REINS_ENDR_APLY_UNIT_PLAN
       set netplanfee    = grsplanfee * v_commissionrate
     where endorno in(select endorno from NBZ_ENDOR_MAIN where policyno=p_policyno);

    update REINS_ENDOR_UNIT
       set netprem    = grsprem * v_commissionrate,
           chgnetprem = chggrsprem * v_commissionrate
     where policyno = p_policyno;
    update REINS_ENDOR_UNIT_PLAN
       set netplanfee    = grsplanfee * v_commissionrate,
           chgnetplanfee = chggrsplanfee * v_commissionrate
     where (ENDORNO, DANGERUNITNO, RERISKCODE, RISKCODE) in
           (select ENDORNO, DANGERUNITNO, RERISKCODE, RISKCODE
            from REINS_ENDOR_UNIT where policyno = p_policyno);

  for rec_reply in cur_reply loop
    v_netcount := 0;
    if rec_reply.reinstype = '0' then
      select sum(netcount) into v_netcount
      from(
      select count(1) netcount
        from REINS_REPLY_SHARE a
       where a.repolicyno = rec_reply.repolicyno
         --and rectimes = rec_reply.rectimes
         and netind = '1'
      union all
      select count(1) netcount
        from REINS_REENDR_SHARE a
       where a.repolicyno = rec_reply.repolicyno
         --and rectimes = rec_reply.rectimes
         and netind = '1');
      --�о��ѷֱ�
      if v_netcount > 0 then
        --����
        grredatacrt.reins_corr(rec_reply.repolicyno,
                               rec_reply.reinstype,
                               rec_reply.restartdate);
        --���·ֱ�������
        update REINS_REPOLICY
           set netprem = grsprem * v_commissionrate
         where repolicyno = rec_reply.repolicyno
           and status in ('0','2');
        --���·���������
        update REINS_REENDOR
           set netprem    = grsprem * v_commissionrate,
               chgnetprem = chggrsprem * v_commissionrate
         where repolicyno = rec_reply.repolicyno
           and status in ('0', '2');
      else
        --û�о��ѷֱ��������
        --���·ֱ�������
        update REINS_REPOLICY
           set netprem = grsprem * v_commissionrate
         where repolicyno = rec_reply.repolicyno;
        --���·���������
        update REINS_REENDOR
           set netprem    = grsprem * v_commissionrate,
               chgnetprem = chggrsprem * v_commissionrate
         where repolicyno = rec_reply.repolicyno;
        --���·ֱ��������
        update REINS_REPLY_SHARE
           set netprem = grsprem * v_commissionrate
         where repolicyno = rec_reply.repolicyno;
        --���·����������
        update REINS_REENDR_SHARE
           set netprem    = grsprem * v_commissionrate,
               chgnetprem = chggrsprem * v_commissionrate
         where repolicyno = rec_reply.repolicyno;
        --���·ֱ��������
        update REINS_RE_PLAN
           set instnetprem = instgrsprem * v_commissionrate
         where repolicyno = rec_reply.repolicyno;
      end if;
    end if;

    if rec_reply.reinstype = '1' then
      select sum(netcount)
        into v_netcount
        from (select count(1) netcount
                from REINS_REPLY_FAC a
               where a.repolicyno = rec_reply.repolicyno
                    --and rectimes = rec_reply.rectimes
                 and netind = '1'
              union all
              select count(1) netcount
                from REINS_REENDR_FAC a
               where a.repolicyno = rec_reply.repolicyno
                    --and rectimes = rec_reply.rectimes
                 and netind = '1');
      if v_netcount > 0 then
        --�о���
        --����
        grredatacrt.reins_corr(rec_reply.repolicyno,
                               rec_reply.reinstype,
                               rec_reply.restartdate);
        --���·ֱ�������
        update REINS_REPOLICY
           set netprem = grsprem * v_commissionrate
         where repolicyno = rec_reply.repolicyno
           and status in ('0','2');
        --���·���������
        update REINS_REENDOR
           set netprem    = grsprem * v_commissionrate,
               chgnetprem = chggrsprem * v_commissionrate
         where repolicyno = rec_reply.repolicyno
           and status in ('0', '2');
      else
        --Frontingë�����㷨
        if v_businesssource = '0050' then
          grredatacrt.reins_corr(rec_reply.repolicyno,
                                 rec_reply.reinstype,
                                 rec_reply.restartdate);
          update REINS_REPOLICY
             set netprem = grsprem * v_commissionrate
           where repolicyno = rec_reply.repolicyno
             and status in ('0','2');
          --���·���������
          update REINS_REENDOR
             set netprem    = grsprem * v_commissionrate,
                 chgnetprem = chggrsprem * v_commissionrate
           where repolicyno = rec_reply.repolicyno
             and status in ('0', '2');
          --�����ٷֲ���
          update REINS_REPLY_FAC a
             set netprem = grsprem * v_commissionrate,
                 commrate = commrate + v_commissionrate2 -
                          (1-decode(grsprem,0,1,netprem/grsprem))*100
           where repolicyno = rec_reply.repolicyno
             and exists( select 1 from REINS_REPOLICY b
                 where a.repolicyno=b.repolicyno
                   and a.rectimes=b.rectimes
                   and status in ('0', '2'));
          --�����ٷֲ���
          update REINS_REENDR_FAC a
             set a.netprem    = a.grsprem * v_commissionrate,
                 a.chgnetprem = a.chggrsprem * v_commissionrate,
                 a.commrate = a.commrate + v_commissionrate2 -
                          (1-decode(a.grsprem,0,1,a.netprem/a.grsprem))*100
           where a.repolicyno = rec_reply.repolicyno
             and exists( select 1 from REINS_REENDOR b
                 where a.repolicyno=b.repolicyno
                   and a.rectimes=b.rectimes
                   and a.reendortimes=b.reendortimes
                   and status in ('0', '2'));
        else
          --��Frontingë�����㷨
          --���·ֱ�������
          update REINS_REPOLICY
             set netprem = grsprem * v_commissionrate
           where repolicyno = rec_reply.repolicyno;
          --���·���������
          update REINS_REENDOR
             set netprem    = grsprem * v_commissionrate,
                 chgnetprem = chggrsprem * v_commissionrate
           where repolicyno = rec_reply.repolicyno;
          --���·ֱ��������
          update REINS_REPLY_SHARE
             set netprem = grsprem * v_commissionrate
           where repolicyno = rec_reply.repolicyno;
          --���·ֱ��������
          update REINS_REENDR_SHARE
             set netprem    = grsprem * v_commissionrate,
                 chgnetprem = chggrsprem * v_commissionrate
           where repolicyno = rec_reply.repolicyno;
          --�����ٷֲ���
          update REINS_REPLY_FAC
             set netprem = grsprem * v_commissionrate
           where repolicyno = rec_reply.repolicyno;
          --�����ٷֲ���
          update REINS_REENDR_FAC
             set netprem    = grsprem * v_commissionrate,
                 chgnetprem = chggrsprem * v_commissionrate
           where repolicyno = rec_reply.repolicyno;
         end if;
      end if;
    end if;
  end loop;
end modify_netprem;


procedure modify_netprem_new(p_policyno in REINS_REPOLICY.policyno%type) is
  cursor cur_reply(p_subPolicyno in reins_repolicy.subpolicyno%type) is
    select *
      from REINS_REPOLICY
     where policyno = p_policyno
       and subpolicyno = p_subPolicyno
       and status not in ('3', '4');
  rec_reply cur_reply%rowtype;
  cursor cur_subPolicy is
    select *
      from nbz_policy_risk t
     where policyno = p_policyno
       and t.riskcode not in
           (select a.codecode
              from pub_code a
             where a.codetype = 'ReinNetPremRisk');
  rec_subPolicy cur_subPolicy%rowtype;

  v_netcount        number(3) := 0;
  v_commissionrate  number := 0;
  v_commissionrate2 number := 0;
  v_businesssource  NBZ_POLICY_MAIN.businesssource%type;
  v_hengsangind     NBZ_POLICY_MAIN.hengsangind%type;
  v_frontingtaxrate NBZ_POLICY_MAIN.frontingritaxpercent%type;
  v_proposalno      NBZ_POLICY_MAIN.proposalno%type;
  v_servicerate     NBZ_POLICY_COMMISSION.commissionpercent%type := 0;
begin
  --ȡӶ�����
  select proposalno,
         businesssource,
         hengsangind,
         nvl(a.frontingritaxpercent, 0)
    into v_proposalno, v_businesssource, v_hengsangind, v_frontingtaxrate
    from NBZ_POLICY_MAIN a
   where policyno = p_policyno;
  for rec_subPolicy in cur_subPolicy loop
    if v_businesssource = '0050' then
      --Frontҵ��ȡӶ�����
      select nvl(sum(commissionpercent), 0)
        into v_commissionrate
        from NBZ_POLICY_COMMISSION
       where policyno = p_policyno
         and subpolicyno = rec_subPolicy.Subpolicyno
         and commissiontype in ('201', '202', '203', '204', '205');
      v_commissionrate2 := v_commissionrate;
    else
      --��Frontҵ��ȡӶ�����
      begin
      select nvl(sum(sumnetpremium) * 100 / sum(sumgrosspremium), 0)
        into v_commissionrate
        from NBZ_POLICY_RISK
       where policyno = p_policyno
         and subpolicyno = rec_subPolicy.Subpolicyno;
       exception
        when others then
          select nvl(sum(sumnetpremium) * 100 / sum(sumgrosspremium), 0)
        into v_commissionrate
        from NBZ_POLICY_COPY_RISK
       where policyno = p_policyno
         and subpolicyno = rec_subPolicy.Subpolicyno
         and endorseqno = '000';
       end;
      v_commissionrate := 100 - v_commissionrate;
    end if;
    --ȡFronting��Ϣ
    if (v_businesssource = '0050' and v_hengsangind != '2') then
      --fronting�Ǻ���

      v_commissionrate := (100 - v_frontingtaxrate - v_commissionrate) / 100;

    elsif (v_businesssource = '0050' and v_hengsangind = '2') then
      --fronting����

      --ȡ��������ѱ���
      begin
        select nvl(commissionpercent, 0)
          into v_servicerate
          from NBZ_POLICY_COMMISSION
         where policyno = rec_reply.policyno
           and subpolicyno = rec_subPolicy.Subpolicyno
           and commissiontype = '212'
           and rownum = 1;
      exception
        when others then
          v_servicerate := 0;
      end;
      v_commissionrate := (100 - v_frontingtaxrate) *
                          (100 - v_commissionrate - v_servicerate) / 10000;
    else
      --��frontingҵ��
      v_commissionrate := (100 - v_commissionrate) / 100;
    end if;

    --�޸�Ͷ�������������������롢����Σ�յ�λ�ľ�����
    select distinct proposalno
      into v_proposalno
      from reins_policy_unit
     where policyno = p_policyno
       and subpolicyno = rec_subPolicy.Subpolicyno;

    update REINS_PROP_UNIT
       set netprem = grsprem * v_commissionrate
     where proposalno = v_proposalno;
    update REINS_PROP_UNIT_PLAN
       set netplanfee = grsplanfee * v_commissionrate
     where proposalno = v_proposalno;

    update REINS_POLICY_UNIT
       set netprem = grsprem * v_commissionrate
     where policyno = p_policyno
       and subpolicyno = rec_subPolicy.Subpolicyno;
    update REINS_POLICY_UNIT_PLAN
       set netplanfee = grsplanfee * v_commissionrate
     where policyno = p_policyno
       and subpolicyno = rec_subPolicy.Subpolicyno;

    update REINS_ENDOR_APPLY_UNIT
       set netprem    = grsprem * v_commissionrate,
           chgnetprem = chggrsprem * v_commissionrate
     where endorno in
           (select endorno from NBZ_ENDOR_MAIN where policyno = p_policyno)
       and subpolicyno = rec_subPolicy.Subpolicyno;
    update REINS_ENDR_APLY_UNIT_PLAN
       set netplanfee = grsplanfee * v_commissionrate
     where endorno in
           (select endorno from NBZ_ENDOR_MAIN where policyno = p_policyno)
       and subpolicyno = rec_subPolicy.Subpolicyno;

    update REINS_ENDOR_UNIT
       set netprem    = grsprem * v_commissionrate,
           chgnetprem = chggrsprem * v_commissionrate
     where policyno = p_policyno
       and subpolicyno = rec_subPolicy.Subpolicyno;
    update REINS_ENDOR_UNIT_PLAN
       set netplanfee    = grsplanfee * v_commissionrate,
           chgnetplanfee = chggrsplanfee * v_commissionrate
     where (ENDORNO, DANGERUNITNO, RERISKCODE, RISKCODE) in
           (select ENDORNO, DANGERUNITNO, RERISKCODE, RISKCODE
              from REINS_ENDOR_UNIT
             where policyno = p_policyno
               and subpolicyno = rec_subPolicy.Subpolicyno);

    for rec_reply in cur_reply(rec_subPolicy.Subpolicyno) loop
      v_netcount := 0;
      if rec_reply.reinstype = '0' then
        select sum(netcount)
          into v_netcount
          from (select count(1) netcount
                  from REINS_REPLY_SHARE a
                 where a.repolicyno = rec_reply.repolicyno
                   and netind = '1'
                union all
                select count(1) netcount
                  from REINS_REENDR_SHARE a
                 where a.repolicyno = rec_reply.repolicyno
                   and netind = '1');
        --�о��ѷֱ�
        if v_netcount > 0 then
          --����
          grredatacrt.reins_corr(rec_reply.repolicyno,
                                 rec_reply.reinstype,
                                 rec_reply.restartdate);
          --���·ֱ�������
          update REINS_REPOLICY
             set netprem = grsprem * v_commissionrate
           where repolicyno = rec_reply.repolicyno
             and status in ('0', '2');
          --���·���������
          update REINS_REENDOR
             set netprem    = grsprem * v_commissionrate,
                 chgnetprem = chggrsprem * v_commissionrate
           where repolicyno = rec_reply.repolicyno
             and status in ('0', '2');
        else
          --û�о��ѷֱ��������
          --���·ֱ�������
          update REINS_REPOLICY
             set netprem = grsprem * v_commissionrate
           where repolicyno = rec_reply.repolicyno;
          --���·���������
          update REINS_REENDOR
             set netprem    = grsprem * v_commissionrate,
                 chgnetprem = chggrsprem * v_commissionrate
           where repolicyno = rec_reply.repolicyno;
          --���·ֱ��������
          update REINS_REPLY_SHARE
             set netprem = grsprem * v_commissionrate
           where repolicyno = rec_reply.repolicyno;
          --���·����������
          update REINS_REENDR_SHARE
             set netprem    = grsprem * v_commissionrate,
                 chgnetprem = chggrsprem * v_commissionrate
           where repolicyno = rec_reply.repolicyno;
          --���·ֱ��������
          update REINS_RE_PLAN
             set instnetprem = instgrsprem * v_commissionrate
           where repolicyno = rec_reply.repolicyno;
        end if;
      end if;

      if rec_reply.reinstype = '1' then
        select sum(netcount)
          into v_netcount
          from (select count(1) netcount
                  from REINS_REPLY_FAC a
                 where a.repolicyno = rec_reply.repolicyno
                      --and rectimes = rec_reply.rectimes
                   and netind = '1'
                union all
                select count(1) netcount
                  from REINS_REENDR_FAC a
                 where a.repolicyno = rec_reply.repolicyno
                      --and rectimes = rec_reply.rectimes
                   and netind = '1');
        if v_netcount > 0 then
          --�о���
          --����
          grredatacrt.reins_corr(rec_reply.repolicyno,
                                 rec_reply.reinstype,
                                 rec_reply.restartdate);
          --���·ֱ�������
          update REINS_REPOLICY
             set netprem = grsprem * v_commissionrate
           where repolicyno = rec_reply.repolicyno
             and status in ('0', '2');
          --���·���������
          update REINS_REENDOR
             set netprem    = grsprem * v_commissionrate,
                 chgnetprem = chggrsprem * v_commissionrate
           where repolicyno = rec_reply.repolicyno
             and status in ('0', '2');
        else
          --Frontingë�����㷨
          if v_businesssource = '0050' then
            grredatacrt.reins_corr(rec_reply.repolicyno,
                                   rec_reply.reinstype,
                                   rec_reply.restartdate);
            update REINS_REPOLICY
               set netprem = grsprem * v_commissionrate
             where repolicyno = rec_reply.repolicyno
               and status in ('0', '2');
            --���·���������
            update REINS_REENDOR
               set netprem    = grsprem * v_commissionrate,
                   chgnetprem = chggrsprem * v_commissionrate
             where repolicyno = rec_reply.repolicyno
               and status in ('0', '2');
            --�����ٷֲ���
            update REINS_REPLY_FAC a
               set netprem  = grsprem * v_commissionrate,
                   commrate = commrate + v_commissionrate2 -
                              (1 - decode(grsprem, 0, 1, netprem / grsprem)) * 100
             where repolicyno = rec_reply.repolicyno
               and exists (select 1
                      from REINS_REPOLICY b
                     where a.repolicyno = b.repolicyno
                       and a.rectimes = b.rectimes
                       and status in ('0', '2'));
            --�����ٷֲ���
            update REINS_REENDR_FAC a
               set a.netprem    = a.grsprem * v_commissionrate,
                   a.chgnetprem = a.chggrsprem * v_commissionrate,
                   a.commrate   = a.commrate + v_commissionrate2 -
                                  (1 - decode(a.grsprem,
                                              0,
                                              1,
                                              a.netprem / a.grsprem)) * 100
             where a.repolicyno = rec_reply.repolicyno
               and exists (select 1
                      from REINS_REENDOR b
                     where a.repolicyno = b.repolicyno
                       and a.rectimes = b.rectimes
                       and a.reendortimes = b.reendortimes
                       and status in ('0', '2'));
          else
            --��Frontingë�����㷨
            --���·ֱ�������
            update REINS_REPOLICY
               set netprem = grsprem * v_commissionrate
             where repolicyno = rec_reply.repolicyno;
            --���·���������
            update REINS_REENDOR
               set netprem    = grsprem * v_commissionrate,
                   chgnetprem = chggrsprem * v_commissionrate
             where repolicyno = rec_reply.repolicyno;
            --���·ֱ��������
            update REINS_REPLY_SHARE
               set netprem = grsprem * v_commissionrate
             where repolicyno = rec_reply.repolicyno;
            --���·ֱ��������
            update REINS_REENDR_SHARE
               set netprem    = grsprem * v_commissionrate,
                   chgnetprem = chggrsprem * v_commissionrate
             where repolicyno = rec_reply.repolicyno;
            --�����ٷֲ���
            update REINS_REPLY_FAC
               set netprem = grsprem * v_commissionrate
             where repolicyno = rec_reply.repolicyno;
            --�����ٷֲ���
            update REINS_REENDR_FAC
               set netprem    = grsprem * v_commissionrate,
                   chgnetprem = chggrsprem * v_commissionrate
             where repolicyno = rec_reply.repolicyno;
          end if;
        end if;
      end if;
    end loop;
  end loop;
end modify_netprem_new;

procedure get_ContainCEM(p_type in varchar2,p_reply_para in reply_para,v_tempCem in out number) is

cursor cur_ttyplan(p_planadj_flag varchar2)
 is
 select openInd,ttyid,PriorityNo,LimitValue,TtyType,StatClass
 from
   (select a.openInd,a.ttyid,a.PriorityNo,0 LimitValue,c.TtyType,c.StatClass
     from REINS_TTY_PLAN a,REINS_TREATY b ,REINS_TTY_TABLE c
     where p_reply_para.startdate between a.startdate and a.enddate
       and a.ttyid=b.ttyid
       and b.ttycode=c.ttycode
       and b.ttystatus in ('2','3')
       --and b.startdate<=p_reply_para.restartdate
       and p_planadj_flag='N'
       --add by 2013-07-11 �Ƿ�ũ������
       --and b.channel_class=p_reply_para.channelClass
       --modified by huangxf 2013/08/23 ����ͨ�������
       AND b.channel_class = DECODE(b.channel_class, '*', '*', p_reply_para.channelClass)
   union all
   select a.openInd,a.ttyid,a.PriorityNo,a.LimitValue,c.TtyType,c.StatClass
     from REINS_PROP_PLAN_ADJ a,REINS_TREATY b,REINS_TTY_TABLE c
    where certino=p_reply_para.certino
      and DangerUnitNo=p_reply_para.DangerUnitNo
      and ReRiskCode=p_reply_para.ReRiskCode
      and RiskCode=p_reply_para.RiskCode
      and a.ttyid=b.ttyid
      and b.ttycode=c.ttycode
      --add by 2013-07-11 �Ƿ�ũ������
      --and b.channel_class=p_reply_para.channelClass
      --modified by huangxf 2013/08/23 ����ͨ�������
      AND b.channel_class = DECODE(b.channel_class, '*', '*', p_reply_para.channelClass)
      --and b.startdate<=p_reply_para.restartdate
      and b.ttystatus in ('2','3')
      and p_planadj_flag='Y'
      and p_type='0'
    union all
   select a.openInd,a.ttyid,a.PriorityNo,a.LimitValue,c.TtyType,c.StatClass
     from REINS_REPLY_PLAN_ADJ a,REINS_TREATY b,REINS_TTY_TABLE c
    where repolicyno=p_reply_para.repolicyno
      and ReEndortimes=p_reply_para.ReEndortimes
      and RecTimes = p_reply_para.RecTimes
      and a.ttyid=b.ttyid
      and b.ttycode=c.ttycode
      --add by 2013-07-11 �Ƿ�ũ������
      --and b.channel_class=p_reply_para.channelClass
      --modified by huangxf 2013/08/23 ����ͨ�������
      AND b.channel_class = DECODE(b.channel_class, '*', '*', p_reply_para.channelClass)
      --and b.startdate<=p_reply_para.restartdate
      and b.ttystatus in ('2','3')
      and p_type='1'
      and p_planadj_flag='Y'
     ) aa
     order by PriorityNo;
  rec_ttyplan cur_ttyplan%rowtype;

v_ttyid varchar2(20);
n_cnt1  number;
v_planadj_flag varchar2(1):='N'; --�Ƿ���ڷֱ��ƻ�����

begin

    if p_type='0' then
     select count(*) into n_cnt1 from REINS_PROP_PLAN_ADJ
      where certino=p_reply_para.certino
        and DangerUnitNo=p_reply_para.DangerUnitNo
        and ReRiskCode=p_reply_para.ReRiskCode
        and RiskCode=p_reply_para.RiskCode;
    elsif p_type='1' then
     select count(*) into n_cnt1 from REINS_REPLY_PLAN_ADJ
      where RePolicyNo=p_reply_para.RePolicyNo
        and reendortimes=p_reply_para.reendortimes;
    end if;

    if n_cnt1>0 then
       v_planadj_flag:='Y';
    end if;

    open cur_ttyplan(v_planadj_flag);
    loop
     fetch cur_ttyplan into rec_ttyplan;
       exit when cur_ttyplan%notfound;

       v_ttyid := rec_ttyplan.ttytype;

       if v_ttyid = '31' and v_tempCem = 0 then
         v_tempCem := 1;
       end if;
    end loop;
    close cur_ttyplan;

end get_ContainCEM;

-- Mantis 0003500 modify by wuwp 2016-08-25 begin
/*ȡ�ϴ��ٷֱַ�δ���ڱ���*/
procedure get_last_fac_rirate_earned(p_repolicyno   in REINS_REPOLICY.repolicyno%type,
                              p_reendortimes in REINS_REPOLICY.reendortimes%type,
                              p_termi_date   in date ,
                              p_BrokerCode in reins_reply_fac.brokercode%type,
                              p_ReinsCode in reins_reply_fac.reinscode%type,
                              p_RiPremLastEarned   out REINS_REPOLICY.Grsprem%type
                       ) is
  cursor cur_sumfee is
     select b.Restartdate,b.ReEndDATE,b.enddate,b.startdate,b.Currency,a.riprem*(trunc(p_termi_date)-trunc(b.startdate))/(trunc(b.enddate)-trunc(b.startdate)+1) as riprem,b.sharerate   --modify  by  wuwp 20160721
     from reins_reply_fac a,reins_repolicy b
    where b.status<>'5'
      and a.repolicyno = b.repolicyno
      and a.rectimes = b.rectimes
      and a.RepolicyNo = p_repolicyno
      and b.restartdate < p_termi_date
      and a.brokercode = p_BrokerCode
      and a.reinscode = p_ReinsCode
  union all
    select d.Restartdate,d.ReEndDATE,d.enddate,d.startdate,d.currency,c.chgriprem*(trunc(p_termi_date)-trunc(d.validdate))/(trunc(d.enddate)-trunc(d.validdate)+1) as riprem,d.sharerate   --modify  by  wuwp 20160721
     from reins_reendr_fac c,reins_reendor d
    where c.RepolicyNo = p_repolicyno
      and c.reendortimes <> p_reendortimes
      and c.repolicyno = d.repolicyno
      and c.reendortimes = d.reendortimes
      and c.rectimes = d.rectimes
      and c.brokercode = p_BrokerCode
      and c.reinscode = p_ReinsCode
      and d.status <> '5'
      and d.restartdate < p_termi_date ;

   rec_sumfee  cur_sumfee%rowtype;

begin
   p_RiPremLastEarned:=0;

   for rec_sumfee in cur_sumfee loop
      p_RiPremLastEarned:=p_RiPremLastEarned + rec_sumfee.riprem  ;
   end loop;
end get_last_fac_rirate_earned;
-- Mantis 0003500 modify by wuwp 2016-08-25 end
/*
ԤԼ��Լ����
*/
procedure openCoverImport(p_policyno   in reins_policy_unit.policyno%type,
                          p_ttyid      in reins_treaty.ttyid%type,
                          p_shareRate  in reins_reply_share.sharerate%type,
                          p_ricommRate in reins_policy_plan_adj.ricommrate%type,
                          p_errCode    out varchar2,
                          p_errDesc    out varchar2) is

  cursor cur_repolicy is
    select distinct repolicyno
      from reins_repolicy
     where policyno = p_policyno;
  rec_repolicy cur_repolicy%rowtype;
  cursor cur_reply is
    select *
      from reins_repolicy
     where policyno = p_policyno
       and status = '0';
  cursor cur_reendor is
    select *
      from reins_reendor a
     where policyno = p_policyno
       and status = '0' order by a.endortimes;
  rec_reendor cur_reendor%rowtype;
  cursor cur_policy is
    select *
      from reins_policy_unit t
     where policyno = p_policyno
       and t.reinsureind = '0';
  rec_policy cur_policy%rowtype;
  errorException exception;
begin
  p_errCode :='T';
  open cur_repolicy;
  fetch cur_repolicy
    into rec_repolicy;
  if cur_repolicy%notfound then
    delete from reins_policy_plan_adj t where t.policyno = p_policyno;
    for rec_policy in cur_policy loop
      insert into reins_policy_plan_adj
        (POLICYNO,
         DANGERUNITNO,
         RERISKCODE,
         RISKCODE,
         TTYID,
         TTYCODE,
         UWYEAR,
         TTYNAME,
         PRIORITYNO,
         LIMITVALUE,
         FLAG,
         OPENIND,
         ENDORTIMES,
         SHARERATE,
         PLANCODE,
         SUBPOLICYNO,
         RICOMMRATE)
      values
        (rec_policy.policyno,
         rec_policy.dangerunitno,
         rec_policy.reriskcode,
         rec_policy.riskcode,
         p_ttyid,
         (select ttycode from reins_treaty where ttyid = p_ttyid),
         (select uwyear from reins_treaty where ttyid = p_ttyid),
         (select TTYABBR from reins_treaty where ttyid = p_ttyid),
         (select priorityno
            from reins_tty_plan
           where ttyid = p_ttyid
             and rownum = 1),
         p_shareRate * rec_policy.pml / 100,
         '',
         'Y',
         '000',
         p_shareRate,
         rec_policy.plancode,
         rec_policy.subpolicyno,
         p_ricommRate);
      --����
      insert into reins_policy_plan_adj
        (POLICYNO,
         DANGERUNITNO,
         RERISKCODE,
         RISKCODE,
         TTYID,
         TTYCODE,
         UWYEAR,
         TTYNAME,
         PRIORITYNO,
         LIMITVALUE,
         FLAG,
         OPENIND,
         ENDORTIMES,
         SHARERATE,
         PLANCODE,
         SUBPOLICYNO,
         RICOMMRATE)
      values
        (rec_policy.policyno,
         rec_policy.dangerunitno,
         rec_policy.reriskcode,
         rec_policy.riskcode,
         'RET01' || to_char(rec_policy.startdate, 'YYYY'),
         'RET01',
         to_char(rec_policy.startdate, 'YYYY'),
         '������Լ',
         (select priorityno
            from reins_tty_plan
           where ttyid = 'RET01' || to_char(rec_policy.startdate, 'YYYY')
             and rownum = 1),
         0,
         '',
         'N',
         '000',
         0,
         rec_policy.plancode,
         rec_policy.subpolicyno,
         0);
       --����
      insert into reins_policy_plan_adj
        (POLICYNO,
         DANGERUNITNO,
         RERISKCODE,
         RISKCODE,
         TTYID,
         TTYCODE,
         UWYEAR,
         TTYNAME,
         PRIORITYNO,
         LIMITVALUE,
         FLAG,
         OPENIND,
         ENDORTIMES,
         SHARERATE,
         PLANCODE,
         SUBPOLICYNO,
         RICOMMRATE)
      values
        (rec_policy.policyno,
         rec_policy.dangerunitno,
         rec_policy.reriskcode,
         rec_policy.riskcode,
         'RET02' || to_char(rec_policy.startdate, 'YYYY'),
         'RET02',
         to_char(rec_policy.startdate, 'YYYY'),
         '����������Լ',
         (select priorityno
            from reins_tty_plan
           where ttyid = 'RET02' || to_char(rec_policy.startdate, 'YYYY')
             and rownum = 1),
         0,
         '',
         'N',
         '000',
         0,
         rec_policy.plancode,
         rec_policy.subpolicyno,
         0);
    end loop;
  end if;
  close cur_repolicy;
  for rec_repolicy in cur_repolicy loop
    --����
    ricde.grredatacrt.reins_corr_new(rec_repolicy.repolicyno, '0', '000');
    commit;
  end loop;
  --�ֱ�
  for rec_reply in cur_reply loop
    delete from reins_reply_plan_adj t
     where t.repolicyno = rec_reply.repolicyno
       and t.reendortimes = '000'
       and t.rectimes = rec_reply.rectimes;
    --������Լ
    insert into reins_reply_plan_adj
      (REPOLICYNO,
       REENDORTIMES,
       RECTIMES,
       LIMITVALUE,
       TTYID,
       TTYCODE,
       UWYEAR,
       TTYNAME,
       PRIORITYNO,
       OPENIND,
       SHARERATE,
       CREATED_BY,
       DATE_CREATED,
       UPDATED_BY,
       DATE_UPDATED,
       RICOMMRATE)
    values
      (rec_reply.repolicyno,
       '000',
       rec_reply.rectimes,
       0,
       'RET01' || to_char(rec_reply.startdate, 'YYYY'),
       'RET01',
       to_char(rec_reply.startdate, 'YYYY'),
       '������Լ',
       (select priorityno
            from reins_tty_plan
           where ttyid = 'RET01' || to_char(rec_reply.startdate, 'YYYY')
             and rownum = 1),
       'N',
       0,
       'system',
       sysdate,
       'system',
       sysdate,
       0);
    --����������Լ
    insert into reins_reply_plan_adj
      (REPOLICYNO,
       REENDORTIMES,
       RECTIMES,
       LIMITVALUE,
       TTYID,
       TTYCODE,
       UWYEAR,
       TTYNAME,
       PRIORITYNO,
       OPENIND,
       SHARERATE,
       CREATED_BY,
       DATE_CREATED,
       UPDATED_BY,
       DATE_UPDATED,
       RICOMMRATE)
    values
      (rec_reply.repolicyno,
       '000',
       rec_reply.rectimes,
       0,
       'RET02' || to_char(rec_reply.startdate, 'YYYY'),
       'RET02',
       to_char(rec_reply.startdate, 'YYYY'),
       '����������Լ',
       (select priorityno
            from reins_tty_plan
           where ttyid = 'RET02' || to_char(rec_reply.startdate, 'YYYY')
             and rownum = 1),
       'N',
       0,
       'system',
       sysdate,
       'system',
       sysdate,
       0);
    --�����ԤԼ��Լ
    insert into reins_reply_plan_adj
      (REPOLICYNO,
       REENDORTIMES,
       RECTIMES,
       LIMITVALUE,
       TTYID,
       TTYCODE,
       UWYEAR,
       TTYNAME,
       PRIORITYNO,
       OPENIND,
       SHARERATE,
       CREATED_BY,
       DATE_CREATED,
       UPDATED_BY,
       DATE_UPDATED,
       RICOMMRATE)
    values
      (rec_reply.repolicyno,
       '000',
       rec_reply.rectimes,
       p_shareRate * rec_reply.pml / 100,
       p_ttyid,
       (select ttycode from reins_treaty where ttyid = p_ttyid),
       (select uwyear from reins_treaty where ttyid = p_ttyid),
       (select TTYABBR from reins_treaty where ttyid = p_ttyid),
       (select priorityno
          from reins_tty_plan
         where ttyid = p_ttyid
           and rownum = 1),
       'Y',
       p_shareRate,
       'system',
       sysdate,
       'system',
       sysdate,
       p_ricommRate);
    ricde.grpropcal.quota_ply_cal(rec_reply.repolicyno,
                                  rec_reply.rectimes,
                                  p_errCode,
                                  p_errDesc);
    commit;
  end loop;
  --����
  for rec_reendor in cur_reendor loop
    delete from reins_reply_plan_adj t
     where t.repolicyno = rec_reendor.repolicyno
       and t.reendortimes = rec_reendor.reendortimes
       and t.rectimes = rec_reendor.rectimes;
    update reins_reendor t
       set t.pml = 1
     where t.pml = 0
       and t.repolicyno = rec_reendor.repolicyno
       and t.reendortimes = rec_reendor.reendortimes
       and t.rectimes = rec_reendor.rectimes;
    insert into reins_reply_plan_adj
      (REPOLICYNO,
       REENDORTIMES,
       RECTIMES,
       LIMITVALUE,
       TTYID,
       TTYCODE,
       UWYEAR,
       TTYNAME,
       PRIORITYNO,
       OPENIND,
       SHARERATE,
       CREATED_BY,
       DATE_CREATED,
       UPDATED_BY,
       DATE_UPDATED,
       RICOMMRATE)
    values
      (rec_reendor.repolicyno,
       rec_reendor.reendortimes,
       rec_reendor.rectimes,
       0,
       'RET01' || to_char(rec_reendor.startdate, 'YYYY'),
       'RET01',
       to_char(rec_reendor.startdate, 'YYYY'),
       '������Լ',
       (select priorityno
          from reins_tty_plan
         where ttyid = 'RET01' || to_char(rec_reendor.startdate, 'YYYY')
           and rownum = 1),
       'N',
       0,
       'system',
       sysdate,
       'system',
       sysdate,
       0);
    insert into reins_reply_plan_adj
      (REPOLICYNO,
       REENDORTIMES,
       RECTIMES,
       LIMITVALUE,
       TTYID,
       TTYCODE,
       UWYEAR,
       TTYNAME,
       PRIORITYNO,
       OPENIND,
       SHARERATE,
       CREATED_BY,
       DATE_CREATED,
       UPDATED_BY,
       DATE_UPDATED,
       RICOMMRATE)
    values
      (rec_reendor.repolicyno,
       rec_reendor.reendortimes,
       rec_reendor.rectimes,
       0,
       'RET02' || to_char(rec_reendor.startdate, 'YYYY'),
       'RET02',
       to_char(rec_reendor.startdate, 'YYYY'),
       '����������Լ',
       (select priorityno
          from reins_tty_plan
         where ttyid = 'RET02' || to_char(rec_reendor.startdate, 'YYYY')
           and rownum = 1),
       'N',
       0,
       'system',
       sysdate,
       'system',
       sysdate,
       0);
    insert into reins_reply_plan_adj
      (REPOLICYNO,
       REENDORTIMES,
       RECTIMES,
       LIMITVALUE,
       TTYID,
       TTYCODE,
       UWYEAR,
       TTYNAME,
       PRIORITYNO,
       OPENIND,
       SHARERATE,
       CREATED_BY,
       DATE_CREATED,
       UPDATED_BY,
       DATE_UPDATED,
       RICOMMRATE)
    values
      (rec_reendor.repolicyno,
       rec_reendor.reendortimes,
       rec_reendor.rectimes,
       p_shareRate * decode(rec_reendor.pml, 0, 1, rec_reendor.pml) / 100,
       p_ttyid,
       (select ttycode from reins_treaty where ttyid = p_ttyid),
       (select uwyear from reins_treaty where ttyid = p_ttyid),
       (select TTYABBR from reins_treaty where ttyid = p_ttyid),
       (select priorityno
          from reins_tty_plan
         where ttyid = p_ttyid
           and rownum = 1),
       'Y',
       p_shareRate,
       'system',
       sysdate,
       'system',
       sysdate,
       p_ricommRate);
    ricde.grpropcal.quota_edr_cal(rec_reendor.repolicyno,
                                  rec_reendor.reendortimes,
                                  rec_reendor.rectimes,
                                  p_errCode,
                                  p_errDesc);
    commit;
  end loop;
  exception
    when errorException then
      p_errCode:='F';
      p_errDesc:= SUBSTR(SQLERRM, 1, 200);
end openCoverImport;

procedure carOpenCover_Proposal(p_proposalno in reins_prop_unit.proposalno%type,
                                p_shareRate  in reins_prop_plan_adj.sharerate%type,
                                p_ricommRate in reins_prop_plan_adj.ricommrate%type,
                                p_ttycode    in reins_prop_plan_adj.ttycode%type,
                                p_flag       in varchar2) is
  cursor cur_prop is
    select * from reins_prop_unit t where t.proposalno = p_proposalno;
  rec_prop cur_prop%rowtype;
  v_ttyid  reins_treaty.ttyid%type;
  errorException exception;
  v_code    varchar2(100);
  v_errDesc varchar2(400);
begin
  delete from reins_prop_plan_adj t where t.certino = p_proposalno;
  if p_flag = '1' then
    v_code  := 'û�к�Լ���ϸ�Ͷ����';
    v_ttyid := get_proposal_ttyid(p_proposalno, p_ttycode);
    if v_ttyid is not null then
      for rec_prop in cur_prop loop
        insert into reins_prop_plan_adj
          (CERTINO,
           DANGERUNITNO,
           RERISKCODE,
           TTYID,
           TTYCODE,
           UWYEAR,
           TTYNAME,
           PRIORITYNO,
           LIMITVALUE,
           FLAG,
           OPENIND,
           RISKCODE,
           SHARERATE,
           PLANCODE,
           SUBCERTINO,
           RICOMMRATE)
        values
          (rec_prop.proposalno,
           rec_prop.dangerunitno,
           rec_prop.reriskcode,
           'RET01' || to_char(rec_prop.startdate, 'YYYY'),
           'RET01',
           to_char(rec_prop.startdate, 'YYYY'),
           '��������Լ',
           (select priorityno
          from reins_tty_plan
         where ttyid = 'RET01' || to_char(rec_prop.startdate, 'YYYY')
           and rownum = 1),
           0,
           '',
           'N',
           rec_prop.riskcode,
           0,
           rec_prop.plancode,
           rec_prop.proposalno,
           0);
        insert into reins_prop_plan_adj
          (CERTINO,
           DANGERUNITNO,
           RERISKCODE,
           TTYID,
           TTYCODE,
           UWYEAR,
           TTYNAME,
           PRIORITYNO,
           LIMITVALUE,
           FLAG,
           OPENIND,
           RISKCODE,
           SHARERATE,
           PLANCODE,
           SUBCERTINO,
           RICOMMRATE)
        values
          (rec_prop.proposalno,
           rec_prop.dangerunitno,
           rec_prop.reriskcode,
           'RET02' || to_char(rec_prop.startdate, 'YYYY'),
           'RET02',
           to_char(rec_prop.startdate, 'YYYY'),
           '����������Լ',
           (select priorityno
          from reins_tty_plan
         where ttyid = 'RET02' || to_char(rec_prop.startdate, 'YYYY')
           and rownum = 1),
           0,
           '',
           'N',
           rec_prop.riskcode,
           0,
           rec_prop.plancode,
           rec_prop.proposalno,
           0);
        --flag=1 ��Ҫ��ԤԼ��Լ�ֳ� flag=0 ����Ҫ
        insert into reins_prop_plan_adj
          (CERTINO,
           DANGERUNITNO,
           RERISKCODE,
           TTYID,
           TTYCODE,
           UWYEAR,
           TTYNAME,
           PRIORITYNO,
           LIMITVALUE,
           FLAG,
           OPENIND,
           RISKCODE,
           SHARERATE,
           PLANCODE,
           SUBCERTINO,
           RICOMMRATE)
        values
          (rec_prop.proposalno,
           rec_prop.dangerunitno,
           rec_prop.reriskcode,
           v_ttyid,
           substr(v_ttyid, 0, 5),
           substr(v_ttyid, 6),
           (select TTYABBR from reins_treaty where ttyid = v_ttyid),
           (select priorityno
              from reins_tty_plan
             where ttyid = v_ttyid
               and rownum = 1),
           p_shareRate * decode(rec_prop.pml, 0, 1, rec_prop.pml) / 100,
           '',
           'Y',
           rec_prop.riskcode,
           p_shareRate,
           rec_prop.plancode,
           rec_prop.proposalno,
           p_ricommRate);
      end loop;
    end if;
  end if;
end carOpenCover_Proposal;

procedure carOpenCover_Endor(p_policyno   in reins_policy_unit.policyno%type,
                             p_shareRate  in reins_prop_plan_adj.sharerate%type,
                             p_ricommRate in reins_prop_plan_adj.ricommrate%type,
                             p_ttycode    in reins_prop_plan_adj.ttycode%type,
                             p_flag       in varchar2) is
  v_ttyid reins_treaty.ttyid%type;
  v_code  varchar2(400);
  v_desc  varchar2(400);
begin
  if p_flag = '1' then
    v_ttyid := get_policy_ttyid(p_policyno,p_ttycode);
    if v_ttyid is not null then
      openCoverImport(p_policyno,v_ttyid,p_shareRate,p_ricommRate,v_code,v_desc);
    end if;
  end if;
end carOpenCover_Endor;

/* ȡ�����ܽ��ĺ�ԼID */
function get_proposal_ttyid(p_proposalno reins_prop_unit.proposalno%type,
                            p_ttyCode reins_treaty.ttycode%type)
    return REINS_TREATY.ttyid%type
 is
    cursor cur_tty is
     select distinct t.ttyid
      from reins_tty_sect_risk t, reins_tty_plan b, reins_prop_unit a
     where t.ttyid = b.ttyid
       and t.reriskcode = a.reriskcode
       and b.startdate <= a.startdate
       and b.enddate >= a.startdate
       and b.ttycode = p_ttyCode
       and a.proposalno = p_proposalno;
    tmp_ttyid REINS_TREATY.ttyid%type;
begin
    open cur_tty;
      fetch cur_tty into tmp_ttyid;
     close cur_tty;
    return tmp_ttyid;
  exception when others then
  return null;
end get_proposal_ttyid;

/* ȡ�����ܽ��ĺ�ԼID */
function get_policy_ttyid(p_policyno reins_policy_unit.policyno%type,
                          p_ttyCode reins_treaty.ttycode%type)
    return REINS_TREATY.ttyid%type
 is
    cursor cur_tty is
     select distinct t.ttyid
      from reins_tty_sect_risk t, reins_tty_plan b, reins_policy_unit a
     where t.ttyid = b.ttyid
       and t.reriskcode = a.reriskcode
       and b.startdate <= a.startdate
       and b.enddate >= a.startdate
       and b.ttycode = p_ttyCode
       and a.proposalno = p_policyno;
    tmp_ttyid REINS_TREATY.ttyid%type;
begin
    open cur_tty;
      fetch cur_tty into tmp_ttyid;
     close cur_tty;
    return tmp_ttyid;
  exception when others then
  return null;
end get_policy_ttyid;


procedure clm_merge(p_payno in varchar2, p_lossSeqNo in varchar2,
                         p_messagecode  out varchar2) is
  cursor cur_merge is
    select *
      from reins_claim_merge
     where payno = p_payno
       and lossseqno = p_lossSeqNo
       and flag = '0';
  rec_merge cur_merge%rowtype;
  cursor cur_policy(p_policyno in varchar2) is
    select * from reins_policy_unit where policyno = p_policyno;
  rec_policy cur_policy%rowtype;
  cursor cur_merge_dtl is
    select distinct t.oripayno, t.orilossseqno
      from reins_claim_merge_dtl t
     where t.payno = p_payno
       and t.lossseqno = p_lossSeqNo;
  rec_merge_dtl cur_merge_dtl%rowtype;

  cursor cur_merge_dtl_claim is
    select distinct t.oriclaimno, t.orireriskcode
      from reins_claim_merge_dtl t
     where t.payno = p_payno
       and t.lossseqno = p_lossSeqNo;
  rec_merge_dtl_claim cur_merge_dtl_claim%rowtype;
  cursor cur_reclaim_ori(p_oriPayno     in varchar2,
                         p_oriLossSeqNo in varchar2) is
    select *
      from reins_reclaim t
     where t.payno = p_oriPayno
       and t.lossseqno = p_oriLossSeqNo
       and t.status = '1';
  rec_reclaim_ori cur_reclaim_ori%rowtype;
  g_errmsg varchar2(400);
  v_char   varchar2(2);
  v_sumpaid reins_claim_unit.sumpaid%type;
  v_sumfee  reins_claim_unit.sumfee%type;
begin
  open cur_merge;
  fetch cur_merge into rec_merge;
  if cur_merge%notfound then
    close cur_merge;
    p_messagecode := 'δ���˵ĺϲ��ⰸ������';
    select '*' into v_char from dual where 1 = 2;
  end if;
  close cur_merge;
  for rec_merge_dtl_claim in cur_merge_dtl_claim loop
    ricde.grxlbill.xriskBill_roll(rec_merge_dtl_claim.oriclaimno,
                                  rec_merge_dtl_claim.orireriskcode,
                                  p_messagecode,
                                  g_errmsg);
  end loop;
  for rec_merge_dtl in cur_merge_dtl loop
    --���ⰸ����
    for rec_reclaim_ori in cur_reclaim_ori(rec_merge_dtl.oripayno,rec_merge_dtl.orilossseqno) loop
      ricde.grredatacrt.reclaim_rollback(rec_reclaim_ori.reclaimno,rec_reclaim_ori.rectimes);
    end loop;
    --�޸�Σ�յ�λ�����ⰸ�Ѿ����Ϊ0
    update reins_claim_unit t
       set t.sumfee  = 0,
           t.sumpaid = 0,
           t.remarks = '�ⰸ�����Ѻϲ�������ţ�' || p_payno || '-' || p_lossSeqNo
     where t.lossseqno = rec_merge_dtl.orilossseqno
       and t.payno = rec_merge_dtl.oripayno;
    --�塢���岻�޸Ľ��
    update reins_reclaim t
    set t.paidsum = 0,
        t.remarks = '�ⰸ�����Ѻϲ�������ţ�' || p_payno || '-' || p_lossSeqNo
    where t.lossseqno = rec_merge_dtl.orilossseqno
      and t.payno = rec_merge_dtl.oripayno
      and t.status not in ('4','3');
  end loop;
  select nvl(sum(t.oripaidsum * decode(a.coinsind, '1', a.baserate / 100, 1)),
           0),
       nvl(sum(t.orisumfee * decode(a.coinsind, '1', a.baserate / 100, 1)),
           0)
  into v_sumpaid, v_sumfee
  from reins_claim_merge_dtl t, reins_claim_unit a
 where t.lossseqno = p_lossseqno
   and t.payno = p_payno
   and t.orilossseqno = a.lossseqno
   and t.oripayno = a.payno;
  --�������ⰸ����
  insert into reins_claim_unit
    (LOSSSEQNO,
     PAYNO,
     POLICYNO,
     DANGERUNITNO,
     CLAIMNO,
     EVENTCODE,
     COMCODE,
     MAKECOMCODE,
     RERISKCODE,
     RISKCODE,
     DANGERCODE,
     ITEMNAME,
     ADDRESSNAME,
     RISKLEVEL,
     RISKLEVELDESC,
     DAMAGEDATE,
     DAMAGECODE,
     DAMAGEREASON,
     INSUREDNAME,
     STARTDATE,
     ENDDATE,
     UWENDDATE,
     CURRENCY,
     SUMLOSS,
     SUMPAID,
     SUMFEE,
     BASERATE,
     COINSIND,
     BUSINESSIND,
     REINSUREIND,
     FLAG,
     DANGERTYPE,
     CREATEDBY,
     CREATEDATE,
     UPDATEDBY,
     UPDATEDATE,
     SPECIALIND,
     ACCCHANNEL,
     PLANCODE,
     SUBPOLICYNO,
     XCALIND,
     REMARKS,
     CLMREINSSTATEAUDIT,
     AUDITORCODEFAC,
     AUDITORCODETTY,
     AUDITDATEFAC,
     AUDITSTATUSFAC,
     AUDITREMARKSFAC,
     AUDITDATETTY,
     AUDITSTATUSTTY,
     AUDITREMARKSTTY,
     INCOINSIND,
     INCOINSRATESUM)
    select rec_merge.lossseqno,
           rec_merge.payno,
           t.POLICYNO,
           t.DANGERUNITNO,
           rec_merge.Claimno,
           t.EVENTCODE,
           t.COMCODE,
           t.MAKECOMCODE,
           t.RERISKCODE,
           t.RISKCODE,
           t.DANGERCODE,
           t.ITEMNAME,
           t.ADDRESSNAME,
           t.RISKLEVEL,
           t.RISKLEVELDESC,
           t.DAMAGEDATE,
           t.DAMAGECODE,
           t.DAMAGEREASON,
           t.INSUREDNAME,
           t.STARTDATE,
           t.ENDDATE,
           t.UWENDDATE,
           t.CURRENCY,
           t.SUMLOSS,
           v_sumpaid,
           v_sumfee,
           100,
           '0',
           t.BUSINESSIND,
           '0',
           t.FLAG,
           t.DANGERTYPE,
           'merge',
           sysdate,
           'merge',
           sysdate,
           t.SPECIALIND,
           t.ACCCHANNEL,
           t.PLANCODE,
           t.SUBPOLICYNO,
           t.XCALIND,
           t.REMARKS,
           t.CLMREINSSTATEAUDIT,
           t.AUDITORCODEFAC,
           t.AUDITORCODETTY,
           t.AUDITDATEFAC,
           t.AUDITSTATUSFAC,
           t.AUDITREMARKSFAC,
           t.AUDITDATETTY,
           t.AUDITSTATUSTTY,
           t.AUDITREMARKSTTY,
           t.INCOINSIND,
           t.INCOINSRATESUM
      from reins_claim_unit t
     where t.policyno = rec_merge.policyno
       and rownum = 1;
  --�޸ĸ��˱�־λ
  update reins_claim_merge set flag = '1'
  where payno = p_payno and lossseqno = p_lossSeqNo;
  --�쳣�ع�
  exception when others then
   p_messagecode := '---------'||sqlerrm;
   dbms_output.put_line('---------'||sqlerrm);
   rollback;
end clm_merge;

end GRPropCal;
