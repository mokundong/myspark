package com.mkd.mahout;
import java.io.File;
import java.util.List;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
/**
 * Created by MKD on 2017/5/25.
 */
public class MyRecommender {
    public static void main(String[] args) {
        try{
            //创建数据类型
            DataModel datamodel = new FileDataModel(new File("D:\\work\\spark-2.1\\datamode.data")); //data
            //创建用户相似度
            UserSimilarity usersimilarity =
                    new PearsonCorrelationSimilarity(datamodel);
            //创建用户近邻
            UserNeighborhood userneighborhood
                    = new ThresholdUserNeighborhood(3.0, usersimilarity, datamodel);
            //创建用户推荐器
            UserBasedRecommender recommender
                    = new GenericUserBasedRecommender(datamodel, userneighborhood,
                    usersimilarity);
            List<RecommendedItem> recommendations = recommender.recommend(2, 3);
            for (RecommendedItem recommendation : recommendations) {
                System.out.println(recommendation);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
