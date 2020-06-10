/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package khipu.consensus.ibft.ibftevent

/** Category of events that will effect and are interpretable by the Ibft processing mechanism */
trait IbftEvent {
  def getType(): IbftEvents.Type
}
