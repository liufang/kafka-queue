<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4 foldmethod=marker: */
// +---------------------------------------------------------------------------
// | SWAN [ $_SWANBR_SLOGAN_$ ]
// +---------------------------------------------------------------------------
// | Copyright $_SWANBR_COPYRIGHT_$
// +---------------------------------------------------------------------------
// | Version  $_SWANBR_VERSION_$
// +---------------------------------------------------------------------------
// | Licensed ( $_SWANBR_LICENSED_URL_$ )
// +---------------------------------------------------------------------------
// | $_SWANBR_WEB_DOMAIN_$
// +---------------------------------------------------------------------------

namespace Kafka;

/**
+------------------------------------------------------------------------------
* Kafka protocol since Kafka v0.8
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class Producer
{
    use \Psr\Log\LoggerAwareTrait;
    use \Kafka\LoggerTrait;

    // {{{ consts
    // }}}
    // {{{ members
    
    private $process = null;

    // }}}
    // {{{ functions
    // {{{ public function __construct()

    private $message = null;

    /**
     * __construct
     *
     * @access public
     * @param $hostList
     * @param null $timeout
     */
    public function __construct(\Closure $producer)
    {
        $this->message = $producer;
        $this->syncMeta();
    }


    /**
     * start producer
     *
     * @access public
     * @return void
     */
    public function send($data = null)
    {
        try {
            $result = $this->doSendMessage($data);
            //success
            if($result && is_callable($this->success)) {
                $this->success($result);
            }
            return $result;
        } catch (\Exception $e) {
            if(is_callable($this->error)) {
                $this->error();
            }
            return false;
        }
    }

    /**
     * 同步服务器端信息
     */
    protected function syncMeta()
    {
        $this->debug('Start sync metadata request');
        $brokerList = explode(',', \Kafka\ProducerConfig::getInstance()->getMetadataBrokerList());
        $brokerHost = array();
        foreach ($brokerList as $key => $val) {
            if (trim($val)) {
                $brokerHost[] = $val;
            }
        }
        if (count($brokerHost) == 0) {
            throw new \Kafka\Exception('Not set config `metadataBrokerList`');
        }

        //初始化通信协议
        // init protocol
        $config = \Kafka\ProducerConfig::getInstance();
        \Kafka\Protocol::init($config->getBrokerVersion(), $this->logger);

        shuffle($brokerHost);
        $broker = \Kafka\ProducerBroker::getInstance();
        foreach ($brokerHost as $host) {
            $socket = $broker->getMetaConnect($host);
            if ($socket) {
                //同步服务器端可用主题以及代理信息
                $params = array();
                $this->debug('Start sync metadata request params:' . json_encode($params));
                $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::METADATA_REQUEST, $params);
                $socket->writeMessage($requestData);
                $message = $socket->readMessage();
                $this->processRequest($broker, $message);
                //发送消息到服务器端
//                return $this->sendMessage($broker);
                return;
            }
        }
        throw new \Kafka\Exception('Not has broker can connection `metadataBrokerList`');
    }

    /**
     * 发送消息
     *
     * @return bool|void
     */
    private function doSendMessage($data = null)
    {
        $this->debug('Start sync metadata request');
        $brokerList = explode(',', \Kafka\ProducerConfig::getInstance()->getMetadataBrokerList());
        $brokerHost = array();
        foreach ($brokerList as $key => $val) {
            if (trim($val)) {
                $brokerHost[] = $val;
            }
        }
        if (count($brokerHost) == 0) {
            throw new \Kafka\Exception('Not set config `metadataBrokerList`');
        }

        //init message
        if(!$data) {
            $data = call_user_func($this->message);
        }

        // init protocol
        $config = \Kafka\ProducerConfig::getInstance();
        \Kafka\Protocol::init($config->getBrokerVersion(), $this->logger);

        shuffle($brokerHost);
        $broker = \Kafka\ProducerBroker::getInstance();
        foreach ($brokerHost as $host) {
            $socket = $broker->getMetaConnect($host);
            if ($socket) {
                //发送消息到服务器端
                return $this->sendMessage($broker, $data);
            }
        }
        throw new \Kafka\Exception('Not has broker can connection `metadataBrokerList`');
    }

    /**
     * 发送消息
     *
     * @param ProducerBroker $broker
     * @param array send data
     * @return bool|void
     * @throws \Exception
     */
    public function sendMessage(\Kafka\ProducerBroker $broker, $data)
    {
        if (empty($data)) {
            return false;
        }

        $sendData = $this->convertMessage($data);
        foreach ($sendData as $brokerId => $topicList) {
            $connect = $broker->getDataConnect($brokerId);
            if (!$connect) {
                return;
            }

            $requiredAck = \Kafka\ProducerConfig::getInstance()->getRequiredAck();
            $params = array(
                'required_ack' => $requiredAck,
                'timeout' => \Kafka\ProducerConfig::getInstance()->getTimeout(),
                'data' => $topicList,
            );
            $this->debug("Send message start, params:" . json_encode($params));
            $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::PRODUCE_REQUEST, $params);
            if ($requiredAck == 0) {
                // If it is 0 the server will not send any response
                $this->state->succRun(\Kafka\Producer\State::REQUEST_PRODUCE);
            } else {
                //发送消息
                $connect->reconnect();
                $connect->writeMessage($requestData);
                //读取响应信息
                $message = $connect->readMessage();
                $this->processRequest($broker, $message);
            }
        }
    }


    /**
     * 解析处理返回结果
     *
     * @param ProducerBroker $broker
     * @param $data
     */
    protected function processRequest(\Kafka\ProducerBroker $broker, $data)
    {
        $correlationId = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
        switch ($correlationId) {
            case \Kafka\Protocol::METADATA_REQUEST:
                $result = \Kafka\Protocol::decode(\Kafka\Protocol::METADATA_REQUEST, substr($data, 4));

                if (!isset($result['brokers']) || !isset($result['topics'])) {
                    $this->error('Get metadata is fail, brokers or topics is null.');
                    throw new \RuntimeException('sync metadate fail');
                } else {
                    $isChange = $broker->setData($result['topics'], $result['brokers']);
                    //TODO 同步变更， 这个信息应该使用一个专门的进程去获取
                }
                break;
            case \Kafka\Protocol::PRODUCE_REQUEST:
                $result = \Kafka\Protocol::decode(\Kafka\Protocol::PRODUCE_REQUEST, substr($data, 4));
                return $result;
                break;
            default:
                $this->error('Error request, correlationId:' . $correlationId);
        }
    }

    /**
     * producer success
     *
     * @access public
     * @return void
     */
    public function success(\Closure $success = null)
    {
        $this->success = $success;
    }

    /**
     * producer error
     *
     * @access public
     * @return void
     */
    public function error(\Closure $error = null)
    {
        $this->error = $error;
    }

    protected function convertMessage($data)
    {
        $sendData = array();
        $broker = \Kafka\ProducerBroker::getInstance();
        $topicInfos = $broker->getTopics();
        foreach ($data as $value) {
            if (!isset($value['topic']) || !trim($value['topic'])) {
                continue;
            }

            if (!isset($topicInfos[$value['topic']])) {
                continue;
            }

            if (!isset($value['value']) || !trim($value['value'])) {
                continue;
            }

            if (!isset($value['key'])) {
                $value['key'] = '';
            }

            $topicMeta = $topicInfos[$value['topic']];
            $partNums = array_keys($topicMeta);
            shuffle($partNums);
            $partId = 0;
            if (!isset($value['partId']) || !isset($topicMeta[$value['partId']])) {
                $partId = $partNums[0];
            } else {
                $partId = $value['partId'];
            }

            $brokerId = $topicMeta[$partId];
            $topicData = array();
            if (isset($sendData[$brokerId][$value['topic']])) {
                $topicData = $sendData[$brokerId][$value['topic']];
            }

            $partition = array();
            if (isset($topicData['partitions'][$partId])) {
                $partition = $topicData['partitions'][$partId];
            }

            $partition['partition_id'] = $partId;
            if (trim($value['key']) != '') {
                $partition['messages'][] = array('value' => $value['value'], 'key' => $value['key']);
            } else {
                $partition['messages'][] = $value['value'];
            }

            $topicData['partitions'][$partId] = $partition;
            $topicData['topic_name'] = $value['topic'];
            $sendData[$brokerId][$value['topic']] = $topicData;
        }

        return $sendData;
    }
}
