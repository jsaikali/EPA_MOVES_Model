-- author wesley faler
-- version 2013-11-12

drop procedure if exists spcheckopmodedistributionimporter;

beginblock
create procedure spcheckopmodedistributionimporter()
begin
	-- mode 0 is run after importing
	-- mode 1 is run to check overall success/failure, allowing data from the default database
	-- mode 2 is run to check overall success/failure, requiring no data from the default database
	declare mode int default ##mode##;

	-- scale 0 is national
	-- scale 1 is single county
	-- scale 2 is project domain
	declare scale int default ##scale##;

	declare howmany int default 0;
	
	-- opmodedistribution
	set howmany=0;
	select count(*) into howmany from opmodedistribution;
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then
		insert into importtempmessages (message)
		select concat('error: source type ',sourcetypeid,', hourdayid ',hourdayid,', link ',linkid,', polprocessid ',polprocessid,' opmodefraction is not 1.0 but instead ',round(sum(opmodefraction),4))
		from opmodedistribution
		join ##defaultdatabase##.operatingmode using (opmodeid)
		group by sourcetypeid, hourdayid, linkid, polprocessid
		having round(sum(opmodefraction),4) <> 1.0000;
	end if;
end
endblock

call spcheckopmodedistributionimporter();
drop procedure if exists spcheckopmodedistributionimporter;
