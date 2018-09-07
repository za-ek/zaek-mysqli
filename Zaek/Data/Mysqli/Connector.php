<?php
namespace Zaek\Data\Mysqli;

use Zaek\Data\Exception\ConnectionError;
use Zaek\Engine\Main;

class Connector extends \Zaek\Data\Connector
{
    /**
     * @var \mysqli|resource
     */
    protected $_link;

    public function __construct(Main $app)
    {
        parent::__construct($app);
    }

    /**
     * @return \mysqli|resource
     */
    public function getLink()
    {
        if ( is_null($this->_link) ) {
            $this->_link = mysqli_connect(
                $this->_app->conf()->get('db_mysqli', 'server'),
                $this->_app->conf()->get('db_mysqli', 'user'),
                $this->_app->conf()->get('db_mysqli', 'password'),
                $this->_app->conf()->get('db_mysqli', 'db')
            );

            if ( !$this->_link ) {
                throw new ConnectionError(mysqli_connect_error());
            }

            mysqli_set_charset($this->_link, 'utf8');
        }
        return $this->_link;
    }

    /**
     * Выборка данных
     *
     * @param $type
     * @param array $aFilter
     * @param array $aRange
     * @param array $aOrder
     * @param array $aLimit
     * @return Result
     * @throws \Zaek\Kernel\Exception\ColumnCountMismatch
     */
    public function select($type, $aFilter = [], $aRange = [], $aOrder = [], $aLimit = [])
    {
        $table = new Result();

        if ( $aRange ) {
            foreach ($aRange as $k => $v) {
                if (is_integer($k)) {
                    $aRange[$k] = "`{$v}` as `{$v}`";
                } else {
                    $aRange[$k] = "`{$k}` as `{$v}`";
                }
            }
        } else {
            $aRange = ['*'];
        }

        $aValues = [];
        if ( $aFilter ) {
            $aValues1 = [];
            foreach ($aFilter as $k => $v) {
                $aFilter[$k] = "`{$k}` = ?";
                $aValues1[] = $v;
            }
            foreach ( $aValues1 as $k => $v ) {
                $aValues[] = &$aValues1[$k];
            }
        } else {
            $aFilter = [1];
        }

        if ( $aOrder ) {
            foreach ($aOrder as $field => $order) {
                $aOrder[$field] = "`{$field}` {$order}";
            }

            $order = " ORDER BY " . implode(',', $aOrder);
        } else {
            $order = '';
        }

        if ( !is_array($aLimit) ) {
            $aLimit = [$aLimit];
        }
        if ( count($aLimit) == 1 || count($aLimit) == 2 ) {
            $limit = ' LIMIT ' . implode(',', $aLimit);
        } else {
            $limit = '';
        }

        $query = "SELECT " . implode(',', $aRange) . " FROM {$type} 
        WHERE " . implode(' AND ', $aFilter) . $order . $limit;

        $stmt = mysqli_prepare($this->getLink(), $query);
        if ( $stmt ) {
            if ( count($aValues) ) {
                call_user_func_array(
                    [$stmt, 'bind_param'],
                    array_merge([str_repeat('s', count($aValues))], $aValues)
                );
            }

            $result = $stmt->execute();
            if ($result) {
                $result = $stmt->get_result();
                $table->setMysqliResult($result);
            }
        } else {
            throw new \LogicException($this->getLink()->error);
        }

        return $table;
    }

    /**
     * Добавление строки
     *
     * @param $type
     * @param $aData
     * @return mixed
     */
    public function insert($type, $aData)
    {
        $link = $this->getLink();

        $query = "INSERT INTO {$type} (".implode(',', array_keys($aData)).") 
                  VALUES (".implode(',', array_fill(0, count($aData), '?')).")";
        $stmt = mysqli_prepare($link, $query);
        if ( $stmt ) {

            $aTmp = [];
            foreach ( $aData as $k => $v ) {
                $aTmp[$k] = $v;
            }
            foreach ( $aTmp as $k => $v ) {
                $aData[$k] = &$aTmp[$k];
            }

            call_user_func_array(
                [$stmt, 'bind_param'],
                array_merge([str_repeat('s', count($aData))], $aData)
            );
            if ($stmt->execute()) {
                return true;
            } else {
                throw new \RuntimeException(mysqli_error($link));
            }
        } else {
            throw new \LogicException(mysqli_error($this->getLink()));
        }
    }

    /**
     * Удаление строк
     *
     * @param $type
     * @param array $aFilter
     * @param array $aOrder
     * @param array $aLimit
     * @return mixed
     */
    public function delete($type, $aFilter = [], $aOrder = [], $aLimit = [])
    {
        $aValues = [];
        if ( $aFilter ) {
            foreach ($aFilter as $k => $v) {
                $aFilter[$k] = "`{$k}` = ?";
                $aValues[] = &$v;
            }
        } else {
            $aFilter = [1];
        }

        if ( $aOrder ) {
            foreach ($aOrder as $field => $order) {
                $aOrder[$field] = "`{$field}` {$order}";
            }

            $order = " ORDER BY " . implode(',', $aOrder);
        } else {
            $order = '';
        }

        if ( !is_array($aLimit) ) {
            $aLimit = [$aLimit];
        }
        if ( count($aLimit) == 1 || count($aLimit) == 2 ) {
            $limit = ' LIMIT ' . implode(',', $aLimit);
        } else {
            $limit = '';
        }

        $query = "DELETE FROM {$type} 
        WHERE " . implode(',', $aFilter) . $order . $limit;

        $stmt = mysqli_prepare($this->getLink(), $query);

        if ( $stmt ) {
            if (count($aValues)) {
                call_user_func_array(
                    [$stmt, 'bind_param'],
                    array_merge([str_repeat('s', count($aValues))], $aValues)
                );
            }

            if ( $stmt->execute() ) {
                return true;
            } else {
                throw new \RuntimeException(mysqli_error($this->getLink()));
            }
        } else {
            throw new \LogicException(mysqli_error($this->getLink()));
        }
    }

    /**
     * Обновление строк
     *
     * @param $type
     * @param $aUpdate
     * @param array $aFilter
     * @param array $aOrder
     * @param array $aLimit
     * @return mixed
     */
    public function update($type, $aUpdate, $aFilter = [], $aOrder = [], $aLimit = [])
    {
        $aValues = [];

        if ( $aFilter ) {
            foreach ($aFilter as $k => $v) {
                $aFilter[$k] = "`{$k}` = ?";
                $aValues[] = $v;
            }
        } else {
            $aFilter = [1];
        }

        if ( $aOrder ) {
            foreach ($aOrder as $field => $order) {
                $aOrder[$field] = "`{$field}` {$order}";
            }

            $order = " ORDER BY " . implode(',', $aOrder);
        } else {
            $order = '';
        }

        if ( !is_array($aLimit) ) {
            $aLimit = [$aLimit];
        }
        if ( count($aLimit) == 1 || count($aLimit) == 2 ) {
            $limit = ' LIMIT ' . implode(',', $aLimit);
        } else {
            $limit = '';
        }

        $query = "UPDATE {$type} SET `" . implode('` = ?,`', array_keys($aUpdate)) . '` = ? '.
            " WHERE " . implode(' AND ', $aFilter) . $order . $limit;

        $stmt = mysqli_prepare($this->getLink(), $query);

        if ( $stmt ) {
            $arr = array_merge(
                [str_repeat('s', count($aUpdate)+count($aValues))],
                $aUpdate,
                $aValues
            );
            $refs = [];
            foreach($arr as $key => $value) {
                $refs[$key] = &$arr[$key];
            }

            call_user_func_array(
                [$stmt, 'bind_param'],
                $refs
            );

            if ( $stmt->execute() ) {
                return true;
            } else {
                throw new \RuntimeException(mysqli_error($this->getLink()));
            }
        } else {
            throw new \LogicException(mysqli_error($this->getLink()));
        }
    }
}