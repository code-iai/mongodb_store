/***************************************************************************
 *  mongodb_log_desig.cpp - MongoDB Logger for designator_integration
 *
 *  Copyright  2012  Jan Winkler
 *  Copyright  2017  Asil Kaan Bozcuoglu,
 *                   Institute for Artificial Intelligence,
 *                   Universität Bremen 
 ****************************************************************************/

/*  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version. A runtime exception applies to
 *  this software (see LICENSE.GPL_WRE file mentioned below for details).
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Library General Public License for more details.
 *
 *  Read the full text in the LICENSE.GPL_WRE file in the doc directory.
 */

// System
#include <iostream>

// ROS
#include <ros/ros.h>

// MongoDB
#include <mongo/client/dbclient.h>
#include <mongodb_store/util.h>

// Designator Integration
#include <designator_integration_msgs/DesignatorRequest.h>
#include <designator_integration_msgs/DesignatorResponse.h>
#include <designators/Designator.h>


using namespace mongo;
using namespace std;

DBClientConnection* dbMongoDB;
string strCollection;
string strTopic;

unsigned int in_counter;
unsigned int out_counter;
unsigned int qsize;
unsigned int drop_counter;

static pthread_mutex_t in_counter_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t out_counter_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t drop_counter_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t qsize_mutex = PTHREAD_MUTEX_INITIALIZER;


enum OperationMode {
  DESIGNATOR = 0,
  DESIGNATOR_REQUEST = 1,
  DESIGNATOR_RESPONSE = 2
};


BSONObj keyValuePairToBSON(designator_integration::KeyValuePair* kvpPair) {
  BSONObjBuilder bobBuilder;
  
  if(kvpPair->type() == designator_integration::KeyValuePair::ValueType::STRING) {
    bobBuilder.append(kvpPair->key(), kvpPair->stringValue());
  } else if(kvpPair->type() == designator_integration::KeyValuePair::ValueType::FLOAT) {
    bobBuilder.append(kvpPair->key(), kvpPair->floatValue());
  } else if(kvpPair->type() == designator_integration::KeyValuePair::ValueType::POSE) {
    geometry_msgs::Pose psPose = kvpPair->poseValue();
    BSONObjBuilder bobTransform;
    bobTransform.append("position",
			BSON(   "x" << psPose.position.x
			     << "y" << psPose.position.y
			     << "z" << psPose.position.z));
    bobTransform.append("orientation",
			BSON(   "x" << psPose.orientation.x
			     << "y" << psPose.orientation.y
			     << "z" << psPose.orientation.z
			     << "w" << psPose.orientation.w));
    bobBuilder.append(kvpPair->key(), bobTransform.obj());
  } else if(kvpPair->type() == designator_integration::KeyValuePair::ValueType::POSESTAMPED) {
    geometry_msgs::PoseStamped psPoseStamped = kvpPair->poseStampedValue();
    Date_t stamp = psPoseStamped.header.stamp.sec * 1000.0 + psPoseStamped.header.stamp.nsec / 1000000.0;
    
    BSONObjBuilder bobTransformStamped;
    BSONObjBuilder bobTransform;
    bobTransformStamped.append("header",
			       BSON(   "seq" << psPoseStamped.header.seq
				    << "stamp" << stamp
				    << "frame_id" << psPoseStamped.header.frame_id));
    bobTransform.append("position",
    			BSON(   "x" << psPoseStamped.pose.position.x
    			     << "y" << psPoseStamped.pose.position.y
    			     << "z" << psPoseStamped.pose.position.z));
    bobTransform.append("orientation",
    			BSON(   "x" << psPoseStamped.pose.orientation.x
    			     << "y" << psPoseStamped.pose.orientation.y
    			     << "z" << psPoseStamped.pose.orientation.z
    			     << "w" << psPoseStamped.pose.orientation.w));
    bobTransformStamped.append("pose", bobTransform.obj());
    bobBuilder.append(kvpPair->key(), bobTransformStamped.obj());
  } else if(kvpPair->type() == designator_integration::KeyValuePair::ValueType::LIST) {
    BSONObjBuilder bobChildren;
    std::list<designator_integration::KeyValuePair*> lstChildren = kvpPair->children();
    
    for(designator_integration::KeyValuePair* kvpChild : lstChildren) {
      bobChildren.appendElements(keyValuePairToBSON(kvpChild));
    }
    
    bobBuilder.append(kvpPair->key(), bobChildren.obj());
  }
  
  return bobBuilder.obj();
}


void logDesignator(designator_integration::Designator* desigLog) {
  BSONObjBuilder bobDesig;
  std::vector<BSONObj> vecChildren;
  
  std::list<designator_integration::KeyValuePair*> lstChildren = desigLog->children();
  
  for(designator_integration::KeyValuePair* kvpChild : lstChildren) {
    bobDesig.appendElements(keyValuePairToBSON(kvpChild));
  }
  
  string strDesigType;
  
  switch(desigLog->type()) {
  case designator_integration::Designator::DesignatorType::ACTION:
    strDesigType = "action";
    break;
    
  case designator_integration::Designator::DesignatorType::OBJECT:
    strDesigType = "object";
    break;
    
  case designator_integration::Designator::DesignatorType::LOCATION:
    strDesigType = "location";
    break;
    
  default:
    strDesigType = "unknown";
    break;
  }
  
  bobDesig.append("_designator_type", strDesigType);
  
  dbMongoDB->insert(strCollection, BSON("designator" << bobDesig.obj() <<
					"__recorded" << Date_t(time(NULL) * 1000) <<
					"__topic" << strTopic));
  
  pthread_mutex_lock(&in_counter_mutex);
  ++in_counter;
  pthread_mutex_unlock(&in_counter_mutex);
  pthread_mutex_lock(&out_counter_mutex);
  ++out_counter;
  pthread_mutex_unlock(&out_counter_mutex);
}


void cbDesignatorResponseMsg(const designator_integration_msgs::DesignatorResponse& msg) {
  std::vector<designator_integration_msgs::Designator> vecDesigs = msg.designators;
  
  for(designator_integration_msgs::Designator desigmsgCurrent : vecDesigs) {
    designator_integration::Designator* desigLog = new designator_integration::Designator(desigmsgCurrent);
    logDesignator(desigLog);
    
    delete desigLog;
  }
}


void cbDesignatorRequestMsg(const designator_integration_msgs::DesignatorRequest& msg) {
  designator_integration::Designator* desigLog = new designator_integration::Designator(msg.designator);
  logDesignator(desigLog);
  
  delete desigLog;
}


void cbDesignatorMsg(const designator_integration_msgs::Designator& msg) {
  designator_integration::Designator* desigLog = new designator_integration::Designator(msg);
  logDesignator(desigLog);
  
  delete desigLog;
}


void printCount(const ros::TimerEvent& te) {
  unsigned int l_in_counter, l_out_counter, l_drop_counter, l_qsize;
  
  pthread_mutex_lock(&in_counter_mutex);
  l_in_counter = in_counter; in_counter = 0;
  pthread_mutex_unlock(&in_counter_mutex);
  
  pthread_mutex_lock(&out_counter_mutex);
  l_out_counter = out_counter; out_counter = 0;
  pthread_mutex_unlock(&out_counter_mutex);
  
  pthread_mutex_lock(&drop_counter_mutex);
  l_drop_counter = drop_counter; drop_counter = 0;
  pthread_mutex_unlock(&drop_counter_mutex);
  
  pthread_mutex_lock(&qsize_mutex);
  l_qsize = qsize; qsize = 0;
  pthread_mutex_unlock(&qsize_mutex);
  
  printf("%u:%u:%u:%u\n", l_in_counter, l_out_counter, l_drop_counter, l_qsize);
  fflush(stdout);
}


void initialize() {
  strCollection = "";
  strTopic = "";
  
  in_counter = 0;
  out_counter = 0;
  drop_counter = 0;
  qsize = 0;
}


int main(int argc, char** argv) {
  int nReturnvalue = 0;
  initialize();
  
  string strMongoDBHostname = "localhost";
  string strNodename = "";
  
  enum OperationMode enOpMode = DESIGNATOR;
  
  char cToken = '\0';
  while((cToken = getopt(argc, argv, "t:m:n:c:d:")) != -1) {
    if((cToken == '?') || (cToken == ':')) {
      printf("Usage: %s -t topic -m mongodb -n nodename -c collection [-d <designator|list>]\n", argv[0]);
      
      nReturnvalue = -1;
      break;
    } else if(cToken == 't') {
      strTopic = optarg;
    } else if(cToken == 'm') {
      strMongoDBHostname = optarg;
    } else if(cToken == 'n') {
      strNodename = optarg;
    } else if(cToken == 'c') {
      strCollection = optarg;
    } else if(cToken == 'd') {
      string strArgument = optarg;
      
      if(strArgument == "designator") {
	enOpMode = DESIGNATOR;
      } else if(strArgument == "designator-request") {
	enOpMode = DESIGNATOR_REQUEST;
      } else if(strArgument == "designator-response") {
	enOpMode = DESIGNATOR_RESPONSE;
      } else {
	// ROS_WARN("Designator mode (-d) was set to `%s' which is invalid.", strArgument.c_str());
	// ROS_WARN("Valid options are: designator, designator-request, designator-response.");
	// ROS_WARN("Assuming the standard value: designator");
      }
    }
  }
  
  if(nReturnvalue == 0) {
    if(strTopic == "") {
      ROS_ERROR("No topic given.");
      nReturnvalue = -2;
    } else if(strNodename == "") {
      ROS_ERROR("No node name given.");
      nReturnvalue = -2;
    } else {
      ros::init(argc, argv, strNodename);
      
      if(ros::ok()) {
	ros::NodeHandle nh;
	
	string strError;
	dbMongoDB = new DBClientConnection(true);
	
	if(dbMongoDB->connect(strMongoDBHostname, strError)) {
	  ros::Subscriber subTopic;
	  
	  switch(enOpMode) {
	  case DESIGNATOR: {
	    subTopic = nh.subscribe(strTopic, 1000, cbDesignatorMsg);
	  } break;
	    
	  case DESIGNATOR_REQUEST: {
	    subTopic = nh.subscribe(strTopic, 1000, cbDesignatorRequestMsg);
	  } break;
	    
	  case DESIGNATOR_RESPONSE: {
	    subTopic = nh.subscribe(strTopic, 1000, cbDesignatorResponseMsg);
	  } break;
	    
	  default: {
	    ROS_ERROR("Invalid topic type selected. This is a serious error which shouldn't be happening.");
	    nReturnvalue = -2;
	  } break;
	  }
	  
	  if(nReturnvalue == 0) {
	    ros::Timer tmrPrintCount = nh.createTimer(ros::Duration(5, 0), printCount);
	    ros::spin();
	  }
	  
	  delete dbMongoDB;
	} else {
	  ROS_ERROR("Failed to connect to MongoDB: %s", strError.c_str());
	  nReturnvalue = -1;
	}
	
	ros::shutdown();
      } else {
	ROS_ERROR("ROS initialization failed.");
      }
    }
  }
  
  return nReturnvalue;
}
