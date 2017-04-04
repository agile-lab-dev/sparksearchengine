package it.agilelab.bigdata.spark.search.impl;

import org.apache.lucene.codecs.*;
import org.apache.lucene.codecs.lucene50.*;
import org.apache.lucene.codecs.lucene53.Lucene53NormsFormat;
import org.apache.lucene.codecs.lucene54.Lucene54DocValuesFormat;
import org.apache.lucene.codecs.lucene60.Lucene60FieldInfosFormat;
import org.apache.lucene.codecs.lucene60.Lucene60PointsFormat;
import org.apache.lucene.codecs.memory.FSTOrdPostingsFormat;


public class CustomCodec extends Codec {
    private final TermVectorsFormat vectorsFormat = new Lucene50TermVectorsFormat();
    private final FieldInfosFormat fieldInfosFormat = new Lucene60FieldInfosFormat();
    private final SegmentInfoFormat segmentInfosFormat = new Lucene50SegmentInfoFormat();
    private final LiveDocsFormat liveDocsFormat = new Lucene50LiveDocsFormat();
    private final CompoundFormat compoundFormat = new Lucene50CompoundFormat();
    private final PostingsFormat postingsFormat = new FSTOrdPostingsFormat();
    private final DocValuesFormat docValuesFormat = new Lucene54DocValuesFormat();
    private final StoredFieldsFormat storedFieldsFormat = new Lucene50StoredFieldsFormat(Lucene50StoredFieldsFormat.Mode.BEST_SPEED);
    private final NormsFormat normsFormat = new Lucene53NormsFormat();
    private final PointsFormat pointsFormat = new Lucene60PointsFormat();

    public CustomCodec() {
        super("CustomCodec");
    }

    @Override
    public final StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    @Override
    public final TermVectorsFormat termVectorsFormat() {
        return vectorsFormat;
    }

    @Override
    public final PostingsFormat postingsFormat() {
        return postingsFormat;
    }

    @Override
    public final FieldInfosFormat fieldInfosFormat() {
        return fieldInfosFormat;
    }

    @Override
    public final SegmentInfoFormat segmentInfoFormat() {
        return segmentInfosFormat;
    }

    @Override
    public final LiveDocsFormat liveDocsFormat() {
        return liveDocsFormat;
    }

    @Override
    public final CompoundFormat compoundFormat() {
        return compoundFormat;
    }

    @Override
    public final DocValuesFormat docValuesFormat() {
        return docValuesFormat;
    }

    @Override
    public final NormsFormat normsFormat() {
        return normsFormat;
    }

    @Override
    public final PointsFormat pointsFormat() {
        return pointsFormat;
    }
}