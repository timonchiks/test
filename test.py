from airflow import DAG
from airflow.contrib.operators.vertica_operator import VerticaOperator
from datetime import datetime, timedelta # импорт необходимых библиотек - неиспользуемые обязательно удалить!

DEFAULT_ARGS = {
    'owner': 'CORA', # наша папка
    'depends_on_past': False,
    'start_date': datetime(2021, 10, 31), # дата написания дага - 1
    'email': ['tlooze@ozon.ru'],  # корпоративная почта, куда будут приходить алерты
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('storage_pay', default_args=DEFAULT_ARGS, schedule_interval="50 6 * * *") # режим запуска - здесь каждый день в 09:45 (так как Москва +3 к Гринвичу)
sql = '''
select	*
INTO LOCAL TEMP TABLE tableq on	COMMIT PRESERVE ROWS
from (
	select
		mbe.AccountingPlaceID,
		mbe.Date,
		s.Name Supplier,
		ap.Name AccountingPlace,
		st.tarif,
		COALESCE(dit.cat1,'Unknown') Category1,
		COALESCE(dit.cat2,'Unknown') Category2,
		(case when amh.in_matrix then Matrix else 'Unknown' end) Matrix,
		cs.ID ContractSupplyID,
		cst.name ContractSupplyType,
		sum(i.Volume * Qty) Volume,
		nvl(arcs.cat1_name_new,dit.cat1) as oldCategory1,
		nvl(arcs.cat2_name_new,dit.cat2) as oldCategory2,
		sum(cost) cost,
		sum(Qty) qty
	from
		CorpAnalytics_team.Shallow_dive_stock mbe
	left join metazonbeeeye.ContractSupply cs on
		mbe.ContractSupplyID = cs.ID
	left join metazonbeeeye.ContractSupplyType cst on 
		cst.id = cs.ContractSupplyTypeID
	left join CorpAnalytics_team.accplaceid_to_clusterid atc
	on atc.AccountingPlaceID = mbe.AccountingPlaceID 
	LEFT JOIN metazonbeeeye.Item i on
		i.RezonItemID = mbe.RezonItemID
	LEFT JOIN metazonbeeeye.ItemType it on
		i.ItemtypeID = it.ID
	left join metazonbeeeye.AccountingPlace ap on
		ap.ID = mbe.AccountingPlaceID
	left join metazonbeeeye.Supplier s on
		s.ID = cs.SupplierID
	left join CatMan_team.ATrifonov_Matrix_history amh on
		mbe.date = amh.Date and mbe.RezonItemID = amh.SKU
	left join (select DISTINCT aa, ac, bd as tarif, gc from CorpAnalytics_team.storage_tarif where ac > 202106 and gc = 3) st on
		st.aa = atc.ClusterID
		and cast(SUBSTRING(CAST(ac as varchar(255)), 1, 4) as int) = YEAR(mbe.date)
		and cast(SUBSTRING(CAST(ac as varchar(255)), 5, 2) as int) = MONTH(mbe.date)
	left join BUhome_team.alekch_rezonitemid_cat_swap arcs on
		mbe.rezonitemid = arcs.RezonItemID
		and nvl(cs.supplierid,-1) = coalesce(arcs.supplierid,cs.SupplierID,-1)
			and mbe.date >= arcs.dt_start
			and mbe.date < arcs.dt_end
		LEFT JOIN ( SELECT
				CommercialCategoryLevel1Name cat1,
				CommercialCategoryLevel2Name cat2,
				ItemGroupLevel4IDMz
			FROM beeeye.dimitemtype) dit on
			dit.ItemGroupLevel4IDmz = i.ItemTypeID
		LEFT JOIN buhome_team.SupplierContractExceptions_pre ex on
			mbe.ContractSupplyID = ex.ContractSupplyID
			and mbe.Date >= ex.dt_start
			and mbe.Date < ex.dt_end
		where it.Category1ID not in (4857855967000, 11157140223310, 3836546441000, 11167918891720, 11251376331490, 4541140014000, 9831613841000, 11031721179210, 3836546439000, 11145606947940)
				and (it.Category1ID not in (11015953539660) or mbe.ContractsupplyID in (1375012193000, 8071508567000, 11035957550020, 9092085746000, 1345767568000, 11202815965410, 1491556211000, 11040732050890, 11032691874030, 11002343784230, 11060103979050, 10041126764000, 808140878000, 11218305603490, 1375012565000, 11281381114460, 11299210614940, 11251368283670, 8405795112000, 11000550172500, 11006117773850, 11029173465940, 11001440038410, 11036065681440, 11060871035900, 11148968798300, 11201852294970, 8529301394000, 11175128443710, 7043983722000, 7201143041000, 8556258248000, 8296443380000, 5666208710000, 11147706172470, 11250298392410, 7465855861000, 11162139707970, 11354522117900, 7955177095000, 7837639397000, 11031186365700))
					and ex.ContractSupplyID is null
					and mbe.AccountingPlaceID in (11191966852830, 11123216658680, 11295067807750, 11123215099520, 11110761768570, 11123213173710, 20141211000, 11295067789650, 11040231255480, 11183454891530)
					and dit.cat1 not in ('Unknown')
						and i.volume < 8000
						and mbe.date >= '2021-07-01'
						group by
							mbe.Date,
							AccountingPlace,
							Category1,
							Category2,
							(case when amh.in_matrix then Matrix else 'Unknown' end),
							cst.name,
							s.Name,
							tarif,
							cs.ID,
							mbe.AccountingPlaceID,
							oldCategory1,
							oldCategory2) qwer;
						
TRUNCATE TABLE CorpAnalytics_team.storage_fee_test;

INSERT
	INTO
	CorpAnalytics_team.storage_fee_test(
	select
	(select mxdate from (select month(date) mon, max(date) mxdate from tableq group by month(date) )mnths where mon = month(date)) date,
	AccountingPlace,
	Category1,
	Category2,
	Matrix,
	(select 'MarketPlace') as ind,
	Supplier,
	ContractSupplyID,
	ContractSupplyType,
	avg(Volume) volume,
	sum((volume*tarif)) as cost_storage_pay,
	avg(cost) cost,
	avg(qty) qty
from tableq
where Month(date)<Month(current_date)
group by 1,2,3,4,5,6,7,8,9
UNION All
select
	date,
	AccountingPlace,
	Category1,
	Category2,
	Matrix,
	(select 'MarketPlace') as ind,
	Supplier,
	ContractSupplyID,
	ContractSupplyType,
	Volume,
	(volume*tarif) cost_storage_pay,
	 cost,
	 qty
from tableq
where Month(date)=Month(current_date)
UNION ALL 
select
	(select mxdate from (select month(date) mon, max(date) mxdate from tableq group by month(date) )mnths where mon = month(date)) date,
	AccountingPlace,
	oldCategory1,
	oldCategory2,
	Matrix,
	(select '1p') as ind,
	Supplier,
	ContractSupplyID,
	ContractSupplyType,
	avg(Volume) volume,
	sum((volume*tarif)) as cost_storage_pay,
	avg(cost) cost,
	avg(qty) qty
from tableq
where Month(date)<Month(current_date)
group by 1,2,3,4,5,6,7,8,9
UNION All
select
	date,
	AccountingPlace,
	oldCategory1,
	oldCategory2,
	Matrix,
	(select '1p') as ind,
	Supplier,
	ContractSupplyID,
	ContractSupplyType,
	Volume,
	((volume*tarif)) cost_storage_pay,
	 cost,
	 qty
from tableq
where Month(date)=Month(current_date)
);
'''  # ваш SQL-скрипт. Нужно записывать данные в уже созданную таблицу
task = VerticaOperator(
    sql=sql,
    task_id='storage_pay',   # название дага
    vertica_conn_id='vertica_prod_CorpAnalytics_team', # название соединения нашей команды
    dag=dag
)
