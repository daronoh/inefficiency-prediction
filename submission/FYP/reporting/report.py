"""Render the dissertation volume from editable prose and verified CSV tables."""
from __future__ import annotations
import json
import re
from pathlib import Path
import pandas as pd
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (BaseDocTemplate, Frame, PageTemplate, Paragraph,
    Spacer, PageBreak, LongTable, TableStyle, Image, KeepTogether)
from reportlab.platypus.tableofcontents import TableOfContents
from FYP.paths import PROJECT_ROOT
from FYP.data.pipeline import write_json
from FYP.reporting.artifacts import LABELS
from FYP.reporting.math import MathRenderer
from FYP.reporting.readability import TABLE_WIDTHS as READING_TABLE_WIDTHS, tables as reading_tables

REPORT_EXTENSIONS=('processed_review_v1','robustness_review_v1','allocation_review_v1','sensitivity_review_v1')

ABSTRACT='''This dissertation studies whether machine learning can choose a few prediction-market events to monitor for structural arbitrage during the next hour. A frozen Dome dataset contains 46 complete-outcome events and five ladders of nested conditions. Models are evaluated across thirteen weekly blocks using only earlier training data. Unknown outcomes remain distinct from hours with no opportunity, and capture is evaluated separately from simulated execution. With three slots, a fixed book-feature random forest retains 94.71% of known positive hours and 99.53% of quoted value, versus 73.54% and 60.76% for ranking current quoted margins. Historical event averages already retain 98.41% of pooled value, but 57.14% when events receive equal weight, compared with the forest's 87.06%. Feature, subject-transfer, training-history and resource comparisons show where the result changes. More complex models do not consistently improve capture. Execution delays create partial-basket losses, and excluding December Fed, which supplies 83.15% of quoted value, removes the forest's simulated profit advantage over quoted gap. Dome's API retirement prevented expansion through the original source. The findings support screening within this cohort, with broader validation limited by coverage, concentration, feedback and execution assumptions.'''


def roman(n):
    result=''
    for value,letter in [(1000,'m'),(900,'cm'),(500,'d'),(400,'cd'),(100,'c'),(90,'xc'),(50,'l'),(40,'xl'),(10,'x'),(9,'ix'),(5,'v'),(4,'iv'),(1,'i')]:
        while n>=value:result+=letter;n-=value
    return result


def report_tables(artifacts):
    tables=artifacts/'tables'
    primary=pd.read_csv(tables/'primary_prediction.csv')
    economic=pd.read_csv(tables/'economic_summary.csv')
    main=economic.loc[economic.scope.eq('all_events')&economic.fold.eq('all')&economic.top_n.eq(3)&economic.arrival_delay_s.eq(.1)]
    p=pd.read_csv(tables/'population.csv').iloc[0]
    population_labels={'competitive_hours_n3':'Competitive hours at N=3'}
    population=pd.DataFrame([dict(Measure=population_labels.get(k,k.replace('_',' ').capitalize()),Value=f'{v:,.0f}' if k not in ['total_peak_value','median_candidates'] else f'{v:,.2f}') for k,v in p.items()])
    prediction=primary[['model','captured_positive_windows','recall','peak_value_recall']].copy()
    prediction['model']=prediction.model.map(LABELS)
    for c in ['recall','peak_value_recall']:prediction[c]=prediction[c].map(lambda x:f'{100*x:.2f}')
    prediction.columns=['Policy','Positive hours captured','Recall (%)','Value capture (%)']
    meta=pd.read_csv(tables/'metadata_comparison.csv')[['method','recall_change','value_change']].copy()
    meta['method']=meta.method.map({'logistic':'Logistic','random_forest':'Random forest','xgboost':'XGBoost'})
    for c in meta.columns[1:]:meta[c]=meta[c].map(lambda x:f'{100*x:+.2f}')
    meta.columns=['Fixed model','Recall change (pp)','Value change (pp)']
    economics=primary[['model']].merge(main[['model','known_pnl_usd','attempts','completed']],on='model')
    for model in ['monitor_all']:
        economics=pd.concat([economics,main.loc[main.model.eq(model),['model','known_pnl_usd','attempts','completed']]],ignore_index=True)
    random=main.loc[main.model.str.startswith('random_')]
    economics=pd.concat([economics,pd.DataFrame([dict(model='Random mean (20 seeds)',known_pnl_usd=random.known_pnl_usd.mean(),attempts=random.attempts.mean(),completed=random.completed.mean())])],ignore_index=True)
    economics['model']=economics.model.map(lambda x:LABELS.get(x,x))
    economics['known_pnl_usd']=economics.known_pnl_usd.map(lambda x:f'{x:,.2f}')
    for c in ['attempts','completed']:economics[c]=economics[c].map(lambda x:f'{x:g}')
    economics.columns=['Policy','Known P&L ($)','Attempts','Complete baskets']
    latency=economic.loc[economic.scope.eq('all_events')&economic.fold.eq('all')&economic.top_n.eq(3)&economic.model.eq('fixed_book_random_forest'),['arrival_delay_s','known_pnl_usd','partial_pnl_usd']].sort_values('arrival_delay_s')
    latency.arrival_delay_s=latency.arrival_delay_s.map(lambda x:f'{1000*x:.0f}')
    for c in ['known_pnl_usd','partial_pnl_usd']:latency[c]=latency[c].map(lambda x:f'{x:,.2f}')
    latency.columns=['Delay (ms)','Known P&L ($)','Partial-basket P&L ($)']
    exclusion=economic.loc[economic.scope.eq('exclude_december_fed')&economic.fold.eq('all')&economic.top_n.eq(3)&economic.arrival_delay_s.eq(.1)&economic.model.isin(['fixed_book_random_forest','quoted_gap_rule']),['model','known_pnl_usd']].copy()
    exclusion.model=exclusion.model.map(LABELS);exclusion.known_pnl_usd=exclusion.known_pnl_usd.map(lambda x:f'{x:,.2f}');exclusion.columns=['Policy after event exclusion','Known P&L ($)']
    result={'population':('Forward ranking population',population),
        'primary':('Opportunity capture at N=3',prediction),
        'metadata':('Matched metadata effects at N=3, percentage points',meta),
        'economics':('Economic comparison at N=3 and 100 ms',economics),
        'latency':('Selected forest: fixed selections across latency scenarios',latency),
        'exclusion':('December Fed exclusion: reranked N=3, 100 ms',exclusion)}
    for directory in REPORT_EXTENSIONS:
        extension=artifacts/'extensions'/directory/'report_tables.json'
        if not extension.exists():continue
        for key,entry in json.loads(extension.read_text()).items():
            if key in result:raise ValueError('Duplicate report table key: '+key)
            result[key]=(entry['caption'],pd.read_csv(artifacts/entry['file'],keep_default_na=False))
    for key,entry in reading_tables(artifacts).items():
        if key in result:raise ValueError('Duplicate reader-aid table key: '+key)
        result[key]=entry
    return result


def build(artifacts=None,output=None):
    artifacts=Path(artifacts or PROJECT_ROOT/'artifacts')
    output=Path(output or PROJECT_ROOT/'report');output.mkdir(parents=True,exist_ok=True)
    identity=json.loads((PROJECT_ROOT/'inputs/report_identity.json').read_text())
    content=(PROJECT_ROOT/'report/content.md').read_text(encoding='utf-8')
    selection=json.loads((artifacts/'research_selection.json').read_text())
    if selection['policy']!='fixed_book_random_forest':
        raise ValueError('The recomputed winning policy differs from the dissertation narrative. Tables and figures are exported; revise the editable report prose before rendering this new result.')
    primary=pd.read_csv(artifacts/'tables/primary_prediction.csv').set_index('model')
    neural=primary.loc['tabm_two_stage']
    content=content.replace('99.04%',f'{100*neural.peak_value_recall:.2f}%').replace('88.02%',f'{100*neural.recall:.2f}%')
    tables=report_tables(artifacts)
    figures=pd.read_csv(artifacts/'figure_index.csv').set_index('number')
    table_widths=dict(READING_TABLE_WIDTHS)
    for directory in REPORT_EXTENSIONS:
        extension=artifacts/'extensions'/directory
        if (extension/'figure_index.csv').exists():
            figures=pd.concat([figures,pd.read_csv(extension/'figure_index.csv').set_index('number')])
        if (extension/'table_widths.json').exists():
            table_widths.update(json.loads((extension/'table_widths.json').read_text()))
    if figures.index.duplicated().any():raise ValueError('Duplicate figure identifiers')
    # References follow placement order, including optional extension tables.
    table_order=re.findall(r'\{\{TABLE:(\w+)\}\}',content)
    figure_order=re.findall(r'\{\{FIGURE:(\d+)\}\}',content)
    for index,name in enumerate(table_order,1):content=content.replace('{{TABLE_NUMBER:'+name+'}}',str(index))
    for index,name in enumerate(figure_order,1):content=content.replace('{{FIGURE_NUMBER:'+name+'}}',str(index))
    if '{{TABLE_NUMBER:' in content or '{{FIGURE_NUMBER:' in content:raise ValueError('Unresolved report cross-reference')
    if len(ABSTRACT.split())>200:raise ValueError('Abstract exceeds 200 words')
    math=MathRenderer()
    body=ParagraphStyle('Body',fontName='Times-Roman',fontSize=12,leading=18,autoLeading='max',spaceAfter=9,alignment=TA_JUSTIFY)
    heading=ParagraphStyle('Chapter',parent=body,fontName='Times-Bold',fontSize=16,leading=22,spaceBefore=12,spaceAfter=16,keepWithNext=True)
    subheading=ParagraphStyle('Section',parent=body,fontName='Times-Bold',fontSize=13,leading=19,spaceBefore=12,spaceAfter=9,keepWithNext=True)
    center=ParagraphStyle('Center',parent=body,alignment=TA_CENTER)
    title=ParagraphStyle('Title',parent=center,fontName='Times-Bold',fontSize=18,leading=25)
    caption_style=ParagraphStyle('Caption',parent=body,fontSize=12,leading=18,alignment=0,spaceBefore=6,spaceAfter=12)
    cell=ParagraphStyle('Cell',parent=body,alignment=0,leading=16,spaceAfter=0)
    toc_style=ParagraphStyle('TOC',parent=body,leftIndent=0,firstLineIndent=0,spaceBefore=6,spaceAfter=3)
    toc_sub=ParagraphStyle('TOCsection',parent=toc_style,leftIndent=18,fontSize=12,leading=18,spaceBefore=1)
    class Document(BaseDocTemplate):
        def __init__(self,*args,**kwargs):
            super().__init__(*args,**kwargs)
            self.addPageTemplates(PageTemplate(id='A4',frames=Frame(self.leftMargin,self.bottomMargin,self.width,self.height,id='body',leftPadding=0,rightPadding=0,topPadding=0,bottomPadding=0),onPageEnd=self.page_end))
        def beforeDocument(self):self.main_start=None;self.reference_start=None;self.appendix_start=None;self.chapter_pages=[]
        def afterFlowable(self,flowable):
            if not isinstance(flowable,Paragraph):return
            text=flowable.getPlainText()
            if flowable.style.name=='Chapter':
                if text=='1 Introduction':self.main_start=self.page
                if text=='References':self.reference_start=self.page
                if text.startswith('Appendix') and self.appendix_start is None:self.appendix_start=self.page
                self.chapter_pages.append(dict(title=text,physical_page=self.page))
            if flowable.style.name in ('Chapter','Section'):
                key=f'h{self.seq.nextf("heading")}';self.canv.bookmarkPage(key)
                level=0 if flowable.style.name=='Chapter' else 1
                self.canv.addOutlineEntry(text,key,level=level,closed=False)
                page=self.page-self.main_start+1 if self.main_start is not None else roman(self.page-2)
                self.notify('TOCEntry',(level,text,page,key))
        def page_end(self,canvas,doc):
            if self.page<=2:return
            label=str(self.page-self.main_start+1) if self.main_start is not None else roman(self.page-2)
            canvas.saveState();canvas.setFont('Times-Roman',12);canvas.drawCentredString(A4[0]/2,15*mm,label);canvas.restoreState()
    doc=Document(str(output/'dissertation.pdf'),pagesize=A4,leftMargin=25*mm,rightMargin=25*mm,topMargin=23*mm,bottomMargin=24*mm,
        title=identity['project_title'],author=identity['student_name'])
    story=[]
    def para(text,style=body):return Paragraph(math.markup(text,style.fontSize),style)
    # The cover and title page are deliberately unnumbered.
    for is_title in [False,True]:
        story.extend([Spacer(1,20*mm),para('B.Comp. Dissertation',center),Spacer(1,20*mm),para(identity['project_title'],title),Spacer(1,7*mm),para('Budgeted event screening for structural arbitrage',center),Spacer(1,18*mm),para('By',center),para(identity['student_name'],center),para(identity['student_number'],center),Spacer(1,17*mm),para(identity['department'],center),para(identity['institution'],center),para(identity['academic_year'],center)])
        if is_title:
            story.extend([Spacer(1,13*mm),para('Project ID: '+identity['project_id'],center),para('Advisor: '+identity['advisor'],center),Spacer(1,8*mm),para('Deliverables: report (one volume); FYP Python package (28 tests); research.ipynb; indexed tables, figures and processed-data extension.',center)])
        story.append(PageBreak())
    story.extend([para('Abstract',title),Spacer(1,6*mm),para(ABSTRACT),para('Subject descriptors: Machine learning; Simulation evaluation; Financial markets.',body),para('Keywords: prediction markets; structural arbitrage; event ranking; order-book replay; tabular learning.',body),para('Implementation: Python 3.12; pandas; scikit-learn; XGBoost; PyTorch/TabM; local parquet data. CUDA acceleration is optional.',body),PageBreak()])
    story.append(para('Table of contents',title));toc=TableOfContents();toc.levelStyles=[toc_style,toc_sub];story.extend([toc,PageBreak()])
    table_number=0;figure_number=0;rendered=[]
    for block in re.split(r'\n\s*\n',content.strip()):
        block=' '.join(block.strip().splitlines())
        if block.startswith('# '):
            text=block[2:]
            if story and not isinstance(story[-1],PageBreak):story.append(PageBreak())
            story.append(para(text,heading));rendered.append('# '+text)
        elif block.startswith('## '):story.append(para(block[3:],subheading));rendered.append(block)
        elif re.fullmatch(r'\{\{TABLE:\w+\}\}',block):
            name=block[len('{{TABLE:'):-2];caption,table=tables[name];table_number+=1
            table.to_csv(artifacts/'tables'/f'report_table_{table_number:02d}.csv',index=False)
            label=f'Table {table_number}. {caption.rstrip(".")}.'
            data_rows=[[Paragraph('<b>'+math.markup(str(c))+'</b>',cell) for c in table]]+[[para(str(v),cell) for v in row] for row in table.itertuples(index=False,name=None)]
            fractions=table_widths.get(name,[.46]+[.54/(len(table.columns)-1)]*(len(table.columns)-1))
            if len(fractions)!=len(table.columns) or abs(sum(fractions)-1)>1e-7:raise ValueError('Invalid table column widths')
            widths=[doc.width*f for f in fractions]
            t=LongTable(data_rows,colWidths=widths,repeatRows=1,hAlign='LEFT')
            t.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('LINEBELOW',(0,0),(-1,0),.7,colors.black),('LINEBELOW',(0,-1),(-1,-1),.5,colors.black),('TOPPADDING',(0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),5),('LEFTPADDING',(0,0),(-1,-1),3),('RIGHTPADDING',(0,0),(-1,-1),3)]))
            story.append(KeepTogether([para(label,caption_style),t,Spacer(1,10)]))
            md='| '+' | '.join(table.columns)+' |\n| '+' | '.join(['---']*len(table.columns))+' |\n'+'\n'.join('| '+' | '.join(str(v) for v in row)+' |' for row in table.itertuples(index=False,name=None))
            rendered.append(label+'\n\n'+md)
        elif re.fullmatch(r'\{\{FIGURE:\d+\}\}',block):
            number=int(block[len('{{FIGURE:'):-2]);r=figures.loc[number]
            figure_number+=1
            from PIL import Image as PILImage
            path=artifacts/r.file
            with PILImage.open(path) as im:w,h=im.size
            width=min(doc.width,doc.width*1.0);height=width*h/w
            if height>150*mm:height=150*mm;width=height*w/h
            label=f'Figure {figure_number}. {r.title}. {r.caption}'
            story.append(KeepTogether([Image(str(path),width=width,height=height),para(label,caption_style)]))
            rendered.append(f'![{label}](../artifacts/{r.file})\n\n{label}')
        elif block.startswith(r'\[') and block.endswith(r'\]'):
            story.append(math.display(block[2:-2].strip()));rendered.append(block)
        else:
            story.append(para(block));rendered.append(block)
    story.extend([PageBreak(),Spacer(1,90*mm),para('B.Comp. Dissertation',center),para(identity['project_title'],center),para(identity['student_name'],center)])
    try:
        doc.multiBuild(story,canvasmaker=math.canvas_class())
    finally:
        math.close()
    if doc.main_start is None or doc.reference_start is None:raise ValueError('Report boundaries missing')
    main_pages=doc.reference_start-doc.main_start
    if main_pages>55:raise ValueError(f'Main report exceeds 55 pages: {main_pages}')
    front=f"# {identity['project_title']}\n\nBudgeted event screening for structural arbitrage\n\n{identity['student_name']} ({identity['student_number']})\n\nProject {identity['project_id']}; {identity['advisor']}; {identity['department']}, {identity['institution']}; {identity['academic_year']}\n\n## Abstract\n\n{ABSTRACT}\n\n"
    (output/'dissertation.md').write_text(front+'\n\n'.join(rendered)+'\n',encoding='utf-8')
    write_json(output/'format_check.json',dict(page_size='A4',body_font='Times-Roman',body_font_size_pt=12,body_leading_pt=18,
        abstract_words=len(ABSTRACT.split()),main_text_pages=main_pages,main_text_limit=55,main_start_physical_page=doc.main_start,
        references_start_physical_page=doc.reference_start,appendix_start_physical_page=doc.appendix_start,chapter_pages=doc.chapter_pages,
        math_rendering='STIX vector mathematics; inline baselines and displayed equations',
        display_equations=math.display_count,
        interpretation='Compiled report draft; author must review claims, citations and institutional requirements before submission.'))
    write_json(output/'math_check.json',math.audit())
    from FYP.reporting.artifacts import finalize
    finalize(artifacts)
    print('Built dissertation:',main_pages,'main-text pages;',len(ABSTRACT.split()),'abstract words.',flush=True)
    return output/'dissertation.pdf'
