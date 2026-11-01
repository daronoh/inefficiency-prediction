"""Export the study's tables and a catalog of fifty distinct scientific figures."""
from __future__ import annotations
import json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from FYP.common import sha256
from FYP.paths import PROJECT_ROOT
from FYP.data.pipeline import FIRST, END, load, write_json
from FYP.experiments.study import OUTPUT as MODELS
from FYP.experiments.economics import OUTPUT as REPLAY
from FYP.models.metrics import ranked
from FYP.models.uncertainty import paired_block_intervals

LABELS={
    'quoted_gap_rule':'Quoted gap','depth_rule':'Depth','freshness_rule':'Freshness',
    'fixed_book_logistic':'Logistic: book','fixed_book_random_forest':'Forest: book',
    'fixed_book_xgboost':'XGBoost: book','fixed_combined_logistic':'Logistic + metadata',
    'fixed_combined_random_forest':'Forest + metadata','fixed_combined_xgboost':'XGBoost + metadata',
    'tuned_combined_logistic':'Tuned logistic','tuned_combined_random_forest':'Tuned forest',
    'tuned_combined_xgboost':'Tuned XGBoost','forest_two_stage':'Forest two-stage',
    'tabm_two_stage':'TabM two-stage','classifier_rank_ensemble':'Classifier ensemble',
    'rank_ensemble':'Logistic / forest / TabM','value_rank_ensemble':'Value ensemble',
    'monitor_all':'Monitor all'}


def generate(models=MODELS,replay=REPLAY,data=None,output=None):
    models,replay=Path(models),Path(replay)
    output=Path(output or PROJECT_ROOT/'artifacts')
    tables=output/'tables';figures=output/'figures'
    tables.mkdir(parents=True,exist_ok=True);figures.mkdir(exist_ok=True)
    frame=load() if data is None else load(data)
    pred=pd.read_parquet(models/'predictions.parquet')
    metrics=pd.read_csv(models/'metrics.csv')
    econ=pd.read_csv(replay/'summary.csv')
    attempts=pd.read_parquet(replay/'attempts.parquet')
    population=pd.read_csv(models/'populations.csv')
    pool=pred.loc[pred.model.eq('quoted_gap_rule')].copy()
    extra=[c for c in frame if c.startswith('x_') and c not in pool]
    pool=pool.merge(frame[['event_slug','time']+extra],on=['event_slug','time'],validate='one_to_one')
    primary=metrics.loc[metrics.scope.eq('all_events')&metrics.fold.eq('all')&metrics.top_n.eq(3)].sort_values(
        ['captured_peak_value','captured_positive_windows','average_precision'],ascending=False,kind='stable')
    BEST=str(primary.iloc[0].model)
    secondary=metrics.loc[metrics.scope.eq('all_events')&metrics.fold.eq('all')&metrics.top_n.eq(5)]
    chosen=pd.read_parquet(models/'selections.parquet')
    all_econ=econ.loc[econ.scope.eq('all_events')&econ.fold.eq('all')]
    main_econ=all_econ.loc[all_econ.top_n.eq(3)&all_econ.arrival_delay_s.eq(.1)]
    taken=attempts.loc[attempts.attempted].copy()
    taken['accounting_time']=taken.arrival_time.where(taken.status.eq('complete'),taken.unwind_time)
    curves=taken.loc[taken.pnl_usd.notna()].groupby(['scope','top_n','model','arrival_delay_s','accounting_time'],as_index=False).pnl_usd.sum().sort_values('accounting_time')
    curves['cumulative_pnl_usd']=curves.groupby(['scope','top_n','model','arrival_delay_s']).pnl_usd.cumsum()
    event_pnl=taken.groupby(['scope','top_n','model','arrival_delay_s','event_slug'],as_index=False).pnl_usd.sum()
    weekly=metrics.loc[metrics.scope.eq('all_events')&metrics.fold.str.startswith('week_')&metrics.top_n.eq(3)]
    paired=[];event_capture=[]
    baseline=ranked(pool,pool.score,3)
    for name,p in pred.groupby('model'):
        s=ranked(p,p.score,3)
        paired.append(dict(model=name,**paired_block_intervals(p,s,baseline)))
        g=p.groupby('event_slug').agg(positive=('target',lambda x:x.eq(1).sum()),peak_value=('value_target','sum'))
        a=s.groupby('event_slug').agg(captured=('target',lambda x:x.eq(1).sum()),captured_value=('value_target','sum'))
        event_capture.append(g.join(a,how='left').fillna(0).reset_index().assign(model=name))
    paired=pd.DataFrame(paired);event_capture=pd.concat(event_capture,ignore_index=True)
    metadata=[]
    for name in ('logistic','random_forest','xgboost'):
        a=primary.loc[primary.model.eq('fixed_book_'+name)].iloc[0]
        b=primary.loc[primary.model.eq('fixed_combined_'+name)].iloc[0]
        metadata.append(dict(method=name,book_recall=a.recall,combined_recall=b.recall,
            recall_change=b.recall-a.recall,book_value_capture=a.peak_value_recall,
            combined_value_capture=b.peak_value_recall,value_change=b.peak_value_recall-a.peak_value_recall))
    metadata=pd.DataFrame(metadata)
    weekly_econ=econ.loc[econ.scope.eq('all_events')&econ.top_n.eq(3)&econ.arrival_delay_s.eq(.1)&econ.fold.str.startswith('week_')]
    grid=weekly_econ.pivot(index='model',columns='fold',values='known_pnl_usd')
    draws=np.random.default_rng(20261031).integers(len(grid.columns),size=(10000,len(grid.columns)))
    paired_pnl=[]
    for name in primary.model:
        difference=grid.loc[name].to_numpy()-grid.loc['quoted_gap_rule'].to_numpy()
        low,high=np.quantile(difference[draws].sum(axis=1),[.025,.975])
        paired_pnl.append(dict(model=name,difference_usd=difference.sum(),lower95=low,upper95=high,blocks=len(grid.columns)))
    paired_pnl=pd.DataFrame(paired_pnl)
    counts=pool.groupby('time').size().reindex(pd.date_range(FIRST,END,freq='1h',inclusive='left'),fill_value=0)
    summary=pd.DataFrame([dict(scheduled_hours=len(counts),event_hours=len(pool),events=pool.event_slug.nunique(),
        known=int(pool.label_known.sum()),unknown=int((~pool.label_known).sum()),positive=int(pool.target.eq(1).sum()),
        positive_events=pool.loc[pool.target.eq(1),'event_slug'].nunique(),median_candidates=counts.median(),
        maximum_candidates=counts.max(),competitive_hours_n3=int(counts.gt(3).sum()),total_peak_value=pool.value_target.sum())])
    exports={'population':summary,'primary_prediction':primary,'secondary_prediction':secondary,
        'all_prediction_metrics':metrics,'economic_summary':econ,'training_populations':population,
        'metadata_comparison':metadata,'weekly_prediction':weekly,'event_capture':event_capture,
        'event_pnl':event_pnl,'paired_capture_intervals':paired,'paired_pnl_intervals':paired_pnl,
        'pnl_curves':curves,'prediction_and_economics':primary.merge(main_econ[['model','known_pnl_usd','attempts','completed','unknown_attempts','partial_pnl_usd','max_drawdown_usd']],on='model')}
    searches=[];settings=[]
    for folder in sorted((models/'tuning').iterdir()):
        searches.append(pd.read_csv(folder/'search.csv').assign(origin=folder.name))
        for method,entry in json.loads((folder/'selected.json').read_text()).items():
            settings.append(dict(origin=folder.name,method=method,**{k:json.dumps(v) if isinstance(v,dict) else v for k,v in entry.items()}))
    exports['tuning_search']=pd.concat(searches,ignore_index=True)
    exports['selected_hyperparameters']=pd.DataFrame(settings)
    for name,df in exports.items():df.to_csv(tables/(name+'.csv'),index=False)
    plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False,'figure.dpi':120})
    catalog=[]
    def save(number,title,caption,df,fig):
        stem=f'{number:02d}_'+''.join(c.lower() if c.isalnum() else '_' for c in title).strip('_')
        fig.suptitle(title,fontsize=14)
        fig.savefig(figures/(stem+'.png'),dpi=150,bbox_inches='tight');plt.close(fig)
        df.to_csv(tables/f'figure_{number:02d}.csv',index=False)
        catalog.append(dict(number=number,title=title,file=f'figures/{stem}.png',source_table=f'tables/figure_{number:02d}.csv',caption=caption))
    def bar(number,title,df,key,caption,percent=False):
        df=df.sort_values(key,ascending=False)
        fig,ax=plt.subplots(figsize=(10,max(4,.34*len(df)+1)),layout='constrained')
        scale=100 if percent else 1
        labels=df['model'].map(lambda x:LABELS.get(x,x)) if 'model' in df else df.iloc[:,0].astype(str)
        ax.barh(labels,df[key]*scale,color=['#c43b32' if x=='quoted_gap_rule' else '#2b719b' for x in df.get('model',pd.Series('',index=df.index))]);ax.invert_yaxis()
        ax.set_xlabel(key.replace('_',' ')+(' (%)' if percent else ''))
        ax.grid(axis='x',alpha=.2)
        save(number,title,caption,df,fig)
    def heat(number,title,df,index,column,value,caption,percent=False):
        grid=df.pivot(index=index,columns=column,values=value)
        fig,ax=plt.subplots(figsize=(12,max(4,len(grid)*.35)),layout='constrained')
        im=ax.imshow(np.ma.masked_invalid(grid.to_numpy(dtype=float)),aspect='auto',cmap='YlGnBu')
        ax.set_yticks(range(len(grid)),[LABELS.get(x,str(x)) for x in grid.index])
        ax.set_xticks(range(len(grid.columns)),[str(x).replace('week_','W') for x in grid.columns],rotation=25)
        if len(grid.columns)<=13:
            for i in range(len(grid)):
                for j in range(len(grid.columns)):
                    v=grid.iloc[i,j];text='NA' if pd.isna(v) else f'{v*100:.0f}%' if percent else f'{v:,.0f}'
                    ax.text(j,i,text,ha='center',va='center',fontsize=7,color='black')
        fig.colorbar(im,ax=ax,label=value.replace('_',' '));save(number,title,caption,df,fig)
    def hist(number,title,column,transform=None):
        values=pool[column].dropna()
        if transform=='log':values=np.log1p(values.clip(lower=0))
        bins=np.histogram_bin_edges(values,bins=40);y,x=np.histogram(values,bins=bins)
        df=pd.DataFrame(dict(left=x[:-1],right=x[1:],count=y))
        fig,ax=plt.subplots(figsize=(9,4),layout='constrained');ax.stairs(y,x,fill=True)
        ax.set_xlabel(('log(1 + x): ' if transform else '')+column);ax.set_ylabel('Event-hours')
        save(number,title,'All eligible forward event-hours, before monitoring selection. Missing feature values excluded.',df,fig)
    # Population and data quality.
    fig,ax=plt.subplots(figsize=(12,3));ax.axis('off')
    stages=['Raw token books','As-of event features','Hourly opportunity targets','Weekly models / top N','Persistent economic replay']
    for i,title in enumerate(stages):
        ax.text(i*.21,.5,title.replace(' ','\n',1),transform=ax.transAxes,ha='center',va='center',bbox=dict(boxstyle='round',fc='#e6f0f5'))
        if i<4:ax.annotate('',xy=(i*.21+.145,.5),xytext=(i*.21+.075,.5),xycoords='axes fraction',arrowprops=dict(arrowstyle='->'))
    save(1,'Research pipeline','Opportunity targets and tuning are independent of execution P&L.',pd.DataFrame(dict(stage=range(1,6),description=stages)),fig)
    fig,ax=plt.subplots(figsize=(12,6),layout='constrained')
    for i,r in population.iterrows():
        start,end=pd.Timestamp(r.start),pd.Timestamp(r.end)
        ax.plot([frame.time.min(),start],[i,i],lw=5,color='#adc8da');ax.plot([start,end],[i,i],lw=5,color='#2b719b')
        if i%4==0 and end-start==pd.Timedelta(days=7):ax.scatter(start,i,color='#c43b32',marker='D',zorder=4)
    ax.set_yticks(range(len(population)),population.origin);ax.set_xlabel('UTC date; light = earlier history, dark = forward block, diamond = retune')
    save(2,'Chronological expanding histories','Full target windows are purged before each origin. Final block is eight hours.',population,fig)
    cohort=json.loads((PROJECT_ROOT/'inputs/cohort.json').read_text())
    cohort_table=pd.DataFrame([dict(type=k,supported=sum(p['stratum']==k for p in cohort),forward=pool.loc[pool.stratum.eq(k),'event_slug'].nunique()) for k in ['negrisk','ladder']])
    fig,ax=plt.subplots(figsize=(7,4),layout='constrained');cohort_table.set_index('type').plot.bar(ax=ax,rot=0);ax.set_ylabel('Events')
    save(3,'Supported and evaluated cohort','Supported membership differs from time-specific availability.',cohort_table,fig)
    extent=frame.groupby('event_slug').time.agg(['min','max']).reset_index()
    fig,ax=plt.subplots(figsize=(12,12),layout='constrained')
    for i,r in extent.iterrows():ax.plot([r['min'],r['max']],[i,i],lw=3)
    ax.set_yticks(range(len(extent)),extent.event_slug,fontsize=6);ax.set_xlabel('First and last eligible scan, UTC; gaps are not implied covered')
    save(4,'Event observation extents','Endpoints are descriptive offline extents, not continuous coverage claims.',extent,fig)
    df=counts.rename('candidates').rename_axis('time').reset_index()
    fig,ax=plt.subplots(figsize=(12,4),layout='constrained');ax.step(df.time,df.candidates,where='post');ax.axhline(3,color='#c43b32',label='N=3');ax.axhline(5,color='gray',ls='--',label='N=5');ax.legend();ax.set_ylabel('Eligible events')
    save(5,'Hourly candidate competition','Candidate counts use the complete eligible pool including unknown labels.',df,fig)
    c=df.assign(week=((df.time-FIRST)//pd.Timedelta(days=7)).astype(int));g=c.groupby('week').candidates.agg(n3=lambda x:x.gt(3).mean(),n5=lambda x:x.gt(5).mean()).reset_index()
    fig,ax=plt.subplots(figsize=(10,4),layout='constrained');ax.plot(g.week,g.n3,label='Pool > 3');ax.plot(g.week,g.n5,label='Pool > 5');ax.set_ylabel('Fraction of scheduled hours');ax.legend()
    save(6,'Weekly monitoring competition','A budget has no exclusion effect when the pool is no larger than the budget.',g,fig)
    daily=pool.assign(day=pool.time.dt.floor('D')).groupby('day').agg(known=('label_known','sum'),total=('label_known','size'),positive=('target',lambda x:x.eq(1).sum())).reset_index();daily['unknown']=daily.total-daily.known
    fig,ax=plt.subplots(figsize=(12,4),layout='constrained');ax.stackplot(daily.day,daily.known,daily.unknown,labels=['Known','Unknown']);ax.legend();ax.set_ylabel('Event-hours')
    save(7,'Outcome coverage over time','Unknown intervals do not become negative examples.',daily,fig)
    daily['positive_fraction']=daily.positive/daily.known.replace(0,np.nan)
    fig,ax=plt.subplots(figsize=(12,4),layout='constrained');ax.plot(daily.day,100*daily.positive_fraction);ax.set_ylabel('Positive / known event-hours (%)')
    save(8,'Opportunity prevalence over time','Undefined when no known outcomes exist.',daily,fig)
    g=pool.groupby('stratum').agg(total=('target','size'),known=('label_known','sum'),positive=('target',lambda x:x.eq(1).sum())).reset_index();g['negative']=g.known-g.positive;g['unknown']=g.total-g.known
    fig,ax=plt.subplots(figsize=(8,4),layout='constrained');g.set_index('stratum')[['positive','negative','unknown']].plot.bar(ax=ax,rot=0,logy=True);ax.set_ylabel('Event-hours (log scale)')
    save(9,'Outcomes by structural event type','The much smaller ladder cohort limits comparisons between event types.',g,fig)
    for number,title,column,transform in [(10,'Quoted gap at ranking','x_best_quoted_net_usd',None),(11,'Mean bid ask spread','x_spread_mean',None),(12,'Displayed ask depth','x_ask_depth_sum','log'),(13,'Age of carried book states','x_book_age_max_s','log'),(14,'Active tokens per event','x_active_tokens',None)]:hist(number,title,column,transform)
    columns=json.loads((models/'design.json').read_text())['feature_views']['book'][:11]
    correlation=pool.reindex(columns=columns).corr() if set(columns).issubset(pool) else frame[columns].corr()
    fig,ax=plt.subplots(figsize=(10,8),layout='constrained');im=ax.imshow(correlation,vmin=-1,vmax=1,cmap='coolwarm');ax.set_xticks(range(len(columns)),[c.removeprefix('x_') for c in columns],rotation=75,ha='right',fontsize=8);ax.set_yticks(range(len(columns)),[c.removeprefix('x_') for c in columns],fontsize=8);fig.colorbar(im,ax=ax)
    save(15,'Current feature correlations','Pearson correlation of current features over all prepared examples; missing pairs excluded.',correlation.rename_axis('feature').reset_index(),fig)
    # Prediction and matched component comparisons.
    for number,title,d,key in [(16,'Positive hour capture at N3',primary,'recall'),(17,'Quoted value capture at N3',primary,'peak_value_recall'),(18,'Positive hour capture at N5',secondary,'recall'),(19,'Quoted value capture at N5',secondary,'peak_value_recall'),(20,'Average precision of opportunity scores',primary,'average_precision'),(21,'Known precision among selected hours',primary,'precision_known'),(22,'Unknown share of selected hours',primary,'selected_unknown_fraction')]:bar(number,title,d,key,'Same eligible forward pool. Known-outcome metrics exclude unknown labels; ranking does not.',True)
    g=primary[['model','selected','candidates']].copy();g['monitoring_fraction']=g.selected/g.candidates
    bar(23,'Event monitoring fraction at N3',g,'monitoring_fraction','Selected event-hours divided by all eligible event-hours, including unknowns.',True)
    g=primary[['model','selected_token_snapshots','all_token_snapshots']].copy();g['token_fraction']=g.selected_token_snapshots/g.all_token_snapshots
    bar(24,'Token monitoring fraction at N3',g,'token_fraction','Token counts reflect different basket sizes; these are not measured compute timings.',True)
    g=pd.concat([primary,secondary])[['model','top_n','capture_lift_vs_random','peak_value_lift_vs_random']]
    heat(25,'Value capture lift over random',g,'model','top_n','peak_value_lift_vs_random','Exact random expectation is computed separately in each scan pool.')
    heat(26,'Weekly positive hour capture',weekly,'model','fold','recall','NA means no known positive target in that week.',True)
    heat(27,'Weekly quoted value capture',weekly,'model','fold','peak_value_recall','NA means zero known target value in that week.',True)
    random=main_econ.loc[main_econ.model.str.startswith('random_')]
    fig,ax=plt.subplots(figsize=(8,4),layout='constrained');ax.hist(random.known_pnl_usd,bins=10);ax.axvline(main_econ.loc[main_econ.model.eq(BEST),'known_pnl_usd'].iloc[0],color='#c43b32',label='Opportunity-selected policy');ax.legend();ax.set_xlabel('Simulated known P&L ($)')
    save(28,'Random policy economic distribution','Twenty fixed random seeds; N=3, 100 ms, uncapped funding.',random,fig)
    for number,title,key in [(29,'Metadata effect on positive capture','recall_change'),(30,'Metadata effect on value capture','value_change')]:bar(number,title,metadata,key,'Combined minus book-only, with fixed identical family settings. Units: percentage points.',True)
    g=pd.DataFrame([dict(method=m,fixed=primary.loc[primary.model.eq('fixed_combined_'+m),'peak_value_recall'].iloc[0],tuned=primary.loc[primary.model.eq('tuned_combined_'+m),'peak_value_recall'].iloc[0]) for m in ['logistic','random_forest','xgboost']])
    fig,ax=plt.subplots(figsize=(8,4),layout='constrained');g.set_index('method').mul(100).plot.bar(ax=ax,rot=0);ax.set_ylabel('Quoted value captured (%)')
    save(31,'Tuning versus fixed combined models','Outer forward performance, N=3; hyperparameters selected using earlier inner blocks.',g,fig)
    keep=['tuned_combined_logistic','tuned_combined_random_forest','tuned_combined_xgboost','forest_two_stage','tabm_two_stage','classifier_rank_ensemble','rank_ensemble','value_rank_ensemble']
    bar(32,'Ensembles and their component models',primary.loc[primary.model.isin(keep)],'peak_value_recall','Equal within-scan percentile-rank averages; N=3.',True)
    contribution=event_capture.loc[event_capture.model.eq(BEST)]
    for number,title,key in [(33,'Positive hours by event','positive'),(34,'Quoted target value by event','peak_value')]:bar(number,title,contribution.loc[contribution[key].gt(0)].drop(columns='model'),key,'Known forward targets only. Value is a sum of hourly maxima, not tradable P&L.')
    excluded=metrics.loc[metrics.scope.eq('exclude_december_fed')&metrics.fold.eq('all')&metrics.top_n.eq(3)]
    for number,title,key in [(35,'Positive capture excluding December Fed','recall'),(36,'Value capture excluding December Fed','peak_value_recall')]:bar(number,title,excluded,key,'Remove the event and rerank the remaining pool at N=3.',True)
    for number,title,key in [(37,'Paired uncertainty in positive capture','recall'),(38,'Paired uncertainty in value capture','value_capture')]:
        d=paired.sort_values(key+'_difference');fig,ax=plt.subplots(figsize=(10,8),layout='constrained');y=np.arange(len(d));ax.hlines(y,100*d[key+'_lower95'],100*d[key+'_upper95']);ax.scatter(100*d[key+'_difference'],y);ax.set_yticks(y,d.model.map(LABELS));ax.axvline(0,color='gray',ls='--');ax.set_xlabel('Difference versus quoted gap (percentage points)')
        save(number,title,'Paired seven-day block bootstrap, 10,000 draws; conditional on fitted policies and cohort; no multiple-comparison adjustment.',d,fig)
    # Economic scenarios, keeping raw dollars distinct from opportunity value.
    for number,n in [(39,3),(40,5)]:
        d=all_econ.loc[all_econ.top_n.eq(n)&all_econ.arrival_delay_s.eq(.1)&~all_econ.model.str.startswith('random_')]
        bar(number,f'Simulated PnL at N{n}',d,'known_pnl_usd','100 ms, uncapped capital/shares; unknown P&L excluded and counted. Dollars, not ROI.')
    for number,n in [(41,3),(42,5)]:
        d=all_econ.loc[all_econ.top_n.eq(n)&~all_econ.model.str.startswith('random_')].copy();d['latency_ms']=d.arrival_delay_s*1000
        heat(number,f'Latency sensitivity at N{n}',d,'model','latency_ms','known_pnl_usd','Fixed first-layer selections; separate persistent ledgers for 0/50/100/250/500 ms. Dollars.')
    core=taken.loc[taken.scope.eq('all_events')&taken.top_n.eq(3)&taken.arrival_delay_s.eq(.1)]
    policies=[BEST,'tuned_combined_logistic','tabm_two_stage','forest_two_stage']
    fig,axes=plt.subplots(2,2,figsize=(12,8),layout='constrained')
    for ax,name in zip(axes.flat,policies):
        for model,color in [('quoted_gap_rule','gray'),(name,'#2b719b')]:
            p=curves.loc[curves.scope.eq('all_events')&curves.top_n.eq(3)&curves.arrival_delay_s.eq(.1)&curves.model.eq(model)]
            v=p.cumulative_pnl_usd.iloc[-1] if len(p) else 0
            ax.step([FIRST]+p.accounting_time.tolist()+[END],[0]+p.cumulative_pnl_usd.tolist()+[v],where='post',label=LABELS[model],color=color)
        ax.set_title(LABELS[name]);ax.set_ylabel('Known P&L ($)');ax.tick_params(axis='x',rotation=20,labelsize=7);ax.legend(fontsize=7)
    save(43,'Cumulative PnL comparisons','N=3, 100 ms; separate panels and a common quoted-gap reference avoid crowded lines.',curves.loc[curves.top_n.eq(3)&curves.arrival_delay_s.eq(.1)&curves.scope.eq('all_events')],fig)
    selected=core.loc[core.model.eq(BEST)].copy();selected['week']=((selected.decision_time-FIRST)//pd.Timedelta(days=7)).astype(int)
    g=selected.groupby(['week','status'],as_index=False).pnl_usd.sum()
    fig,ax=plt.subplots(figsize=(10,4),layout='constrained');g.pivot(index='week',columns='status',values='pnl_usd').fillna(0).plot.bar(ax=ax,rot=0);ax.set_ylabel('Known P&L ($)')
    save(44,'Completed and partial basket contributions','Selected forest, N=3, 100 ms; observed partial fills are unwound after one second.',g,fig)
    g=core.groupby(['model','status']).size().rename('attempts').reset_index()
    fig,ax=plt.subplots(figsize=(12,max(7,.34*g.model.nunique())),layout='constrained');g.pivot(index='model',columns='status',values='attempts').fillna(0).rename(index=LABELS).plot.barh(stacked=True,ax=ax);ax.set_xlabel('Attempt count');ax.legend(fontsize=8)
    save(45,'Execution outcomes by policy','N=3, 100 ms; attempts only. Zero imposed random failures.',g,fig)
    for number,title,key,log in [(46,'Selected forest order quantities','quantity',True),(47,'Selected forest per attempt PnL','pnl_usd',False)]:
        values=selected[key].dropna();v=np.log1p(values) if log else values
        y,x=np.histogram(v,bins=35);d=pd.DataFrame(dict(left=x[:-1],right=x[1:],count=y))
        fig,ax=plt.subplots(figsize=(9,4),layout='constrained');ax.stairs(y,x,fill=True);ax.set_xlabel(('log(1 + shares)' if log else 'Known P&L ($)'));ax.set_ylabel('Attempts')
        save(number,title,'Selected forest, N=3, 100 ms. Available depth bounds quantity despite uncapped capital.',d,fig)
    fig,ax=plt.subplots(figsize=(11,4),layout='constrained');drawdowns=[]
    for name in [BEST,'quoted_gap_rule','forest_two_stage']:
        p=curves.loc[curves.scope.eq('all_events')&curves.top_n.eq(3)&curves.arrival_delay_s.eq(.1)&curves.model.eq(name)].copy()
        p['drawdown']=p.cumulative_pnl_usd.clip(lower=0).cummax()-p.cumulative_pnl_usd
        ax.step(p.accounting_time,p.drawdown,where='post',label=LABELS[name]);drawdowns.append(p)
    ax.legend();ax.set_ylabel('Drawdown ($)')
    save(48,'Drawdown under primary execution','Known-P&L accounting, starting at zero. Does not include unresolved unknown losses.',pd.concat(drawdowns,ignore_index=True),fig)
    d=econ.loc[econ.fold.eq('all')&econ.top_n.eq(3)&econ.arrival_delay_s.eq(.1)&econ.model.isin([BEST,'quoted_gap_rule'])]
    fig,axes=plt.subplots(1,2,figsize=(11,4),layout='constrained');d.pivot(index='model',columns='scope',values='known_pnl_usd').rename(index=LABELS).plot.bar(ax=axes[0],rot=15);axes[0].set_ylabel('Known P&L ($)');axes[0].legend(fontsize=7)
    ep=event_pnl.loc[event_pnl.scope.eq('all_events')&event_pnl.top_n.eq(3)&event_pnl.arrival_delay_s.eq(.1)&event_pnl.model.eq(BEST)].sort_values('pnl_usd',ascending=False).head(6)
    axes[1].barh(ep.event_slug,ep.pnl_usd);axes[1].invert_yaxis();axes[1].tick_params(axis='y',labelsize=6);axes[1].set_xlabel('Selected forest P&L ($)')
    save(49,'Economic concentration and event exclusion','Exclusion reranks the pool. The leading event materially changes the economic interpretation.',d,fig)
    d=main_econ.loc[~main_econ.model.str.startswith('random_')].copy()
    d=d.sort_values('known_pnl_usd',ascending=False)
    fig,axes=plt.subplots(1,2,figsize=(13,8),sharey=True,layout='constrained')
    positions=np.arange(len(d))
    axes[0].barh(positions,d.no_settlement_funding_usd/1e6);axes[0].set_xlabel('No-settlement funding ($ millions)')
    axes[1].barh(positions,d.known_pnl_usd);axes[1].set_xlabel('Known P&L ($)')
    axes[0].set_yticks(positions,d.model.map(LABELS));axes[0].invert_yaxis()
    save(50,'Funding and simulated performance','No settlement cash is released during the experiment; funding is a scenario, not measured deployed capital or an ROI denominator.',d,fig)
    if len(catalog)!=50 or len({r['number'] for r in catalog})!=50:raise ValueError('Incomplete figure catalog')
    pd.DataFrame(catalog).to_csv(output/'figure_index.csv',index=False)
    pd.DataFrame([dict(file=p.relative_to(output).as_posix(),rows=len(pd.read_csv(p))) for p in sorted(tables.glob('*.csv'))]).to_csv(output/'table_index.csv',index=False)
    write_json(output/'research_selection.json',dict(policy=BEST,objective='Pooled peak quoted value capture at N=3',
        selection='Retrospective chronological policy comparison; no execution P&L used for fitting or tuning.',
        latest_bundle=(models/'folds/week_12/bundle.json').relative_to(PROJECT_ROOT).as_posix()))
    write_json(output/'figures_manifest.json',dict(files_sha256={p.relative_to(output).as_posix():sha256(p) for p in [*tables.glob('*.csv'),*figures.glob('*.png'),output/'figure_index.csv',output/'table_index.csv']}))
    print('Exported',len(list(tables.glob('*.csv'))),'tables and',len(catalog),'figures.',flush=True)
    return catalog


def finalize(output):
    """Index report tables added after figure generation and seal all exports."""
    output=Path(output)
    tables=output/'tables';figures=output/'figures'
    pd.DataFrame([dict(file=p.relative_to(output).as_posix(),rows=len(pd.read_csv(p)))
        for p in sorted(tables.glob('*.csv'))]).to_csv(output/'table_index.csv',index=False)
    paths=[*tables.glob('*.csv'),*figures.glob('*.png'),output/'figure_index.csv',output/'table_index.csv']
    write_json(output/'figures_manifest.json',dict(files_sha256={p.relative_to(output).as_posix():sha256(p) for p in paths}))
