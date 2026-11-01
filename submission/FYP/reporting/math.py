"""Typeset inline and displayed mathematics as vector paths in the PDF.

Matplotlib supplies STIX mathematical glyphs and TeX layout. ReportLab retains
paragraph wrapping and baselines; its image slots are replaced by vector forms
at drawing time. Temporary transparent images only reserve layout dimensions.
They are never embedded as equation screenshots or shipped as extra figures.
"""
from __future__ import annotations
from functools import lru_cache
from html import escape
from pathlib import Path
import re
from tempfile import TemporaryDirectory

from matplotlib.font_manager import FontProperties
from matplotlib.mathtext import MathTextParser
from matplotlib.path import Path as GlyphPath
from matplotlib.textpath import TextPath
from PIL import Image
from reportlab.pdfgen.canvas import Canvas
from reportlab.platypus import Flowable


@lru_cache(maxsize=2048)
def glyph_outline(filename, size, codepoint):
    return TextPath((0, 0), chr(codepoint), size=size,
                    prop=FontProperties(fname=filename), usetex=False)


def draw_outline(canvas, outline):
    path=canvas.beginPath()
    current=(0.,0.)
    for points,code in outline.iter_segments(curves=True,simplify=False):
        if code==GlyphPath.MOVETO:
            path.moveTo(*points);current=tuple(points)
        elif code==GlyphPath.LINETO:
            path.lineTo(*points);current=tuple(points)
        elif code==GlyphPath.CURVE3:
            control,end=points[:2],points[2:]
            a=[current[i]+2*(control[i]-current[i])/3 for i in (0,1)]
            b=[end[i]+2*(control[i]-end[i])/3 for i in (0,1)]
            path.curveTo(*a,*b,*end);current=tuple(end)
        elif code==GlyphPath.CURVE4:
            path.curveTo(*points);current=tuple(points[-2:])
        elif code==GlyphPath.CLOSEPOLY:
            path.close()
        else:
            raise ValueError(f'Unsupported mathematical outline segment: {code}')
    canvas.drawPath(path,fill=1,stroke=0)


class Equation:
    def __init__(self, expression, size, parser):
        self.expression,self.size=expression,size
        self.layout=parser.parse('$'+expression+'$',dpi=72,
            prop=FontProperties(size=size,math_fontfamily='stix'))
        self.width=float(self.layout.width)
        self.height=float(self.layout.height)
        self.depth=float(self.layout.depth)
        if not self.width or not self.height:
            raise ValueError('Mathematics must have a nonempty layout')

    def draw(self, canvas, x, baseline):
        canvas.saveState();canvas.translate(x,baseline);canvas.setFillColorRGB(0,0,0)
        for glyph in self.layout.glyphs:
            # Matplotlib 3.11 adds a glyph index after the Unicode codepoint.
            if len(glyph)==6:font,size,codepoint,_,ox,oy=glyph
            else:font,size,codepoint,ox,oy=glyph
            canvas.saveState();canvas.translate(float(ox),float(oy))
            draw_outline(canvas,glyph_outline(font.fname,float(size),int(codepoint)))
            canvas.restoreState()
        for x0,y0,w,h in self.layout.rects:
            canvas.rect(float(x0),float(y0),float(w),float(h),fill=1,stroke=0)
        canvas.restoreState()


class DisplayEquation(Flowable):
    def __init__(self, equation, number):
        super().__init__();self.equation=equation;self.number=number
        self.spaceBefore=6;self.spaceAfter=12

    def wrap(self, available_width, available_height):
        if self.equation.width>available_width-40:
            raise ValueError('Displayed equation is too wide; split it in the report source')
        self.width=available_width;self.height=self.equation.height+8
        return self.width,self.height

    def draw(self):
        e=self.equation
        e.draw(self.canv,(self.width-e.width)/2,4+e.depth)
        self.canv.setFont('Times-Roman',12)
        self.canv.drawRightString(self.width,4+e.depth,f'({self.number})')


class MathRenderer:
    def __init__(self):
        self.parser=MathTextParser('path')
        self.temporary=TemporaryDirectory(prefix='fyp_math_')
        self.equations={};self.images={};self.inline_count=0;self.display_count=0

    def equation(self, expression, size=12):
        key=(expression,size)
        if key not in self.equations:
            self.equations[key]=Equation(expression,size,self.parser)
        return self.equations[key]

    def inline(self, expression, size=12):
        equation=self.equation(expression,size)
        key=(expression,size)
        if key not in self.images:
            name=Path(self.temporary.name)/f'{len(self.images):04d}.png'
            Image.new('RGBA',(1,1),(0,0,0,0)).save(name)
            self.images[key]=(str(name),equation)
        name,equation=self.images[key];self.inline_count+=1
        return (f'<img src="{escape(name,quote=True)}" width="{equation.width}" '
                f'height="{equation.height}" valign="{-equation.depth}"/>')

    def markup(self, text, size=12):
        def prose(value):
            value=escape(value)
            value=re.sub(r'\*\*(.+?)\*\*',r'<b>\1</b>',value)
            value=re.sub(r'`(.+?)`',r'<i>\1</i>',value)
            # Preserve mathematical operators occurring in ordinary prose too.
            for char,tex in [('−','-'),('≥',r'\geq'),('≤',r'\leq'),('→',r'\to')]:
                if char in value:value=value.replace(char,self.inline(tex,size))
            return value
        result=[];last=0
        for match in re.finditer(r'\\\((.+?)\\\)',text):
            result.extend([prose(text[last:match.start()]),self.inline(match.group(1),size)])
            last=match.end()
        result.append(prose(text[last:]))
        return ''.join(result)

    def display(self, expression):
        self.display_count+=1
        return DisplayEquation(self.equation(expression),self.display_count)

    def canvas_class(self):
        renderer=self
        class MathCanvas(Canvas):
            def drawImage(self,image,x,y,width=None,height=None,**kwargs):
                filename=getattr(image,'fileName',None)
                match=next(((i,e) for i,(name,e) in enumerate(renderer.images.values()) if filename==name),None)
                if match is None:return super().drawImage(image,x,y,width,height,**kwargs)
                i,e=match;form=f'fyp_math_{i}'
                if not self.hasForm(form):
                    self.beginForm(form,0,-e.depth,e.width,e.height-e.depth)
                    e.draw(self,0,0);self.endForm()
                self.saveState();self.translate(x,y)
                self.scale(width/e.width,height/e.height)
                self.translate(0,e.depth);self.doForm(form);self.restoreState()
                return e.width,e.height
        return MathCanvas

    def audit(self):
        return dict(renderer='Matplotlib mathtext / STIX vector outlines',
            inline_occurrences=self.inline_count,display_equations=self.display_count,
            formulas=[dict(tex=e.expression,size_pt=e.size,width_pt=e.width,
                height_pt=e.height,depth_pt=e.depth) for e in self.equations.values()],
            rasterized_equations=False)

    def close(self):self.temporary.cleanup()
